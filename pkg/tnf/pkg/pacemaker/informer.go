package pacemaker

import (
	"context"
	"fmt"

	pacmkrv1 "github.com/openshift/api/etcd/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// NewClusterInformer builds a SharedIndexInformer for the singleton
// PacemakerCluster CR. The informer is unstarted; the caller must Run it on
// a context.
//
// This is the same informer construction used inside NewHealthCheck. It is
// exported as a standalone helper so out-of-cluster tooling (e.g. the
// tnf-auto-taint-polyfill cmd binary) can consume PacemakerCluster events
// without bringing up the full HealthCheck controller machinery.
func NewClusterInformer(restConfig *rest.Config) (cache.SharedIndexInformer, error) {
	restClient, err := createPacemakerRESTClient(restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create REST client: %w", err)
	}

	scheme := runtime.NewScheme()
	if err := pacmkrv1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("failed to add scheme for informer: %w", err)
	}

	informer := cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				result := &pacmkrv1.PacemakerClusterList{}
				err := restClient.Get().
					Resource(PacemakerResourceName).
					VersionedParams(&options, runtime.NewParameterCodec(scheme)).
					Do(context.Background()).
					Into(result)
				if err != nil {
					klog.Errorf("PacemakerCluster list failed: %v", err)
				}
				return result, err
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				watcher, err := restClient.Get().
					Resource(PacemakerResourceName).
					VersionedParams(&options, runtime.NewParameterCodec(scheme)).
					Watch(context.Background())
				if err != nil {
					klog.Errorf("PacemakerCluster watch failed: %v", err)
				}
				return watcher, err
			},
		},
		&pacmkrv1.PacemakerCluster{},
		HealthCheckResyncInterval,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
	)
	return informer, nil
}
