package operator

import (
	"context"
	"os"
	"sync/atomic"
	"time"

	pacmkrv1 "github.com/openshift/api/etcd/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"github.com/openshift/cluster-etcd-operator/pkg/tnf/pkg/pacemaker"
	"github.com/openshift/cluster-etcd-operator/pkg/tnf/pkg/tools"
)

// Tunables for the auto-taint controller. These are package-level vars (not
// constants) so tests can override them; for production callers they are
// effectively constants.
//
// TODO: surface notReadyApplyThreshold via Etcd.spec before GA; see the
// graduation criteria in
// enhancements/two-node-fencing/auto-out-of-service-taint.md.
var (
	notReadyApplyThreshold = 3 * time.Minute
	taintReconcilerPeriod  = 30 * time.Second
)

// pacemakerInformerRef holds a reference to the PacemakerCluster informer once
// runPacemakerControllers has created it. Read by the taint reconciler on each
// tick. nil-safe: a nil/unsynced informer disables the Pacemaker fast path and
// the reconciler falls back to notReadyApplyThreshold.
var pacemakerInformerRef atomic.Pointer[cache.SharedIndexInformer]

// autoTaintEnabled gates the entire feature at runtime. Reads the env-var
// TNF_AUTO_OUT_OF_SERVICE_TAINT; treats any value other than "true" as
// disabled.
//
// TODO: replace with a real featuregates.FeatureGate check against
// TNFAutoOutOfServiceTaint once the feature-gate accessor is plumbed into
// HandleDualReplicaClusters.
func autoTaintEnabled() bool {
	return os.Getenv("TNF_AUTO_OUT_OF_SERVICE_TAINT") == "true"
}

// startTaintReconciler launches a goroutine that, every taintReconcilerPeriod,
// iterates over control-plane nodes and applies or removes the out-of-service
// taint as appropriate. The reconciler is a no-op while autoTaintEnabled
// returns false, so this is safe to call unconditionally.
func startTaintReconciler(
	ctx context.Context,
	kubeClient kubernetes.Interface,
	controlPlaneNodeLister corev1listers.NodeLister,
) {
	go wait.UntilWithContext(ctx, func(ctx context.Context) {
		if !autoTaintEnabled() {
			return
		}
		nodes, err := controlPlaneNodeLister.List(labels.Everything())
		if err != nil {
			klog.Warningf("auto-taint: list control-plane nodes failed: %v", err)
			return
		}
		now := time.Now()
		for _, n := range nodes {
			reconcileNodeTaint(ctx, kubeClient, n, now)
		}
	}, taintReconcilerPeriod)
}

// reconcileNodeTaint evaluates a single Node and brings its out-of-service
// taint into the desired state. Safe to call from event handlers as a fast
// path, in addition to the periodic reconciler.
func reconcileNodeTaint(ctx context.Context, kubeClient kubernetes.Interface, n *corev1.Node, now time.Time) {
	if n == nil {
		return
	}
	if tools.IsNodeReady(n) {
		if hasOutOfServiceTaint(n) {
			klog.Infof("auto-taint: %s is Ready; removing out-of-service taint", n.Name)
			if err := removeOutOfServiceTaint(ctx, kubeClient, n.Name); err != nil {
				klog.Warningf("auto-taint: remove failed for %s: %v", n.Name, err)
			}
		}
		return
	}
	apply, reason := shouldApplyOutOfServiceTaint(n, now, notReadyApplyThreshold, currentPacemakerLookup())
	if !apply {
		return
	}
	if hasOutOfServiceTaint(n) {
		return
	}
	klog.Infof("auto-taint: applying out-of-service taint to %s (reason=%s)", n.Name, reason)
	if err := applyOutOfServiceTaint(ctx, kubeClient, n.Name); err != nil {
		klog.Warningf("auto-taint: apply failed for %s: %v", n.Name, err)
	}
}

// currentPacemakerLookup returns a PacemakerLookup that dereferences the
// shared informer pointer at call time. Returning a fresh lookup on every
// invocation keeps the reconciler decoupled from the informer's lifecycle.
func currentPacemakerLookup() PacemakerLookup {
	return func() (*pacmkrv1.PacemakerCluster, bool) {
		ptr := pacemakerInformerRef.Load()
		if ptr == nil {
			return nil, false
		}
		return pacemaker.LookupCurrentClusterState(*ptr)
	}
}
