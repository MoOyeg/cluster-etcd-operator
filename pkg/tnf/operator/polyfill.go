package operator

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"github.com/openshift/cluster-etcd-operator/pkg/tnf/pkg/pacemaker"
	"github.com/openshift/cluster-etcd-operator/pkg/tnf/pkg/tools"
)

// RunPolyfill is a standalone entry point that exercises the auto-taint
// controller (the same gate function, helpers, and reconciler used by the
// production cluster-etcd-operator) against a live cluster, without rebuilding
// or redeploying CEO.
//
// It is intended for the cmd/tnf-auto-taint-polyfill binary and for
// integration smoke tests. The function:
//
//   - builds a control-plane Node informer (filtered by the standard
//     control-plane role labels),
//   - builds a PacemakerCluster informer and publishes it into
//     pacemakerInformerRef so the gate consults real Pacemaker fence state,
//   - hooks reconcileNodeTaint into AddFunc and UpdateFunc as a fast path,
//   - starts the same periodic reconciler used in production.
//
// Honors TNF_AUTO_OUT_OF_SERVICE_TAINT exactly as the production controller
// does: with the env-var unset, the reconciler ticks but does nothing. Set
// TNF_AUTO_OUT_OF_SERVICE_TAINT=true to make it active.
//
// Returns when ctx is cancelled or when the informers fail to sync.
func RunPolyfill(ctx context.Context, restConfig *rest.Config) error {
	kubeClient, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("build kube client: %w", err)
	}

	// Node informer scoped to control-plane nodes via label selector. This
	// mirrors the production controller's controlPlaneNodeInformer.
	factory := informers.NewSharedInformerFactoryWithOptions(
		kubeClient,
		30*time.Second,
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.LabelSelector = labelControlPlane + ",!" + "node-role.kubernetes.io/worker"
		}),
	)
	nodeInformer := factory.Core().V1().Nodes().Informer()
	nodeLister := corev1listers.NewNodeLister(nodeInformer.GetIndexer())

	// PacemakerCluster informer, same construction as the production controller.
	pcInformer, err := pacemaker.NewClusterInformer(restConfig)
	if err != nil {
		return fmt.Errorf("build PacemakerCluster informer: %w", err)
	}
	pacemakerInformerRef.Store(&pcInformer)

	// Periodic reconciler — same goroutine production runs.
	startTaintReconciler(ctx, kubeClient, nodeLister)

	// Fast-path event handlers — same logic as the production AddFunc/UpdateFunc.
	if _, err := nodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			n, ok := obj.(*corev1.Node)
			if !ok {
				return
			}
			reconcileNodeTaint(ctx, kubeClient, n, time.Now())
		},
		UpdateFunc: func(oldObj, newObj any) {
			oldNode, oldOk := oldObj.(*corev1.Node)
			newNode, newOk := newObj.(*corev1.Node)
			if !oldOk || !newOk {
				return
			}
			if tools.IsNodeReady(oldNode) != tools.IsNodeReady(newNode) {
				reconcileNodeTaint(ctx, kubeClient, newNode, time.Now())
			}
		},
	}); err != nil {
		return fmt.Errorf("attach node informer handler: %w", err)
	}

	klog.Infof("polyfill: starting informers (auto-taint enabled=%v)", autoTaintEnabled())
	go pcInformer.Run(ctx.Done())
	go nodeInformer.Run(ctx.Done())

	if !cache.WaitForCacheSync(ctx.Done(), nodeInformer.HasSynced) {
		return fmt.Errorf("node informer failed to sync")
	}
	klog.Infof("polyfill: node informer synced; %d control-plane node(s) visible",
		len(nodeInformer.GetStore().List()))

	// PacemakerCluster informer may fail to sync on clusters where the CRD is
	// absent (e.g. non-TNF clusters used for negative-control testing). Treat
	// sync failure as a warning, not a fatal — the gate falls back to the
	// NotReady-duration path.
	syncCtx, syncCancel := context.WithTimeout(ctx, 30*time.Second)
	defer syncCancel()
	if !cache.WaitForCacheSync(syncCtx.Done(), pcInformer.HasSynced) {
		klog.Warningf("polyfill: PacemakerCluster informer did not sync in 30s; running with threshold-only gate")
	} else {
		klog.Infof("polyfill: PacemakerCluster informer synced")
	}

	<-ctx.Done()
	return nil
}
