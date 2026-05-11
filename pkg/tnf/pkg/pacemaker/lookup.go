package pacemaker

import (
	pacmkrv1 "github.com/openshift/api/etcd/v1"
	"k8s.io/client-go/tools/cache"
)

// LookupCurrentClusterState reads the singleton PacemakerCluster CR from the
// given shared informer's store. Returns (cr, true) on success.
//
// Returns (nil, false) if the informer is nil, has not yet synced, the
// singleton key is absent, or the stored object is not a PacemakerCluster.
// Callers should treat (nil, false) as "no authoritative fence-state signal
// available right now" rather than as an error — typical reasons are CEO
// startup before the pacemaker informer is initialized (see
// runPacemakerControllers in pkg/tnf/operator/starter.go) and brief windows
// after restart.
func LookupCurrentClusterState(informer cache.SharedIndexInformer) (*pacmkrv1.PacemakerCluster, bool) {
	if informer == nil {
		return nil, false
	}
	if !informer.HasSynced() {
		return nil, false
	}
	item, exists, err := informer.GetStore().GetByKey(PacemakerClusterResourceName)
	if err != nil || !exists {
		return nil, false
	}
	pc, ok := item.(*pacmkrv1.PacemakerCluster)
	if !ok {
		return nil, false
	}
	return pc, true
}
