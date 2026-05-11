package operator

import (
	"time"

	pacmkrv1 "github.com/openshift/api/etcd/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// labelControlPlane is the standard role label distinguishing control-plane
// nodes. The historical "master" label is also accepted because clusters that
// predate the rename keep it for compatibility.
const (
	labelControlPlane = "node-role.kubernetes.io/control-plane"
	labelMaster       = "node-role.kubernetes.io/master"
)

// PacemakerLookup returns the current PacemakerCluster CR if the informer is
// ready, or (nil, false) if the data is not yet available. A nil PacemakerLookup
// is allowed; it disables the Pacemaker fast-path and leaves only the
// NotReady-duration fallback.
type PacemakerLookup func() (*pacmkrv1.PacemakerCluster, bool)

// ApplyReason identifies why shouldApplyOutOfServiceTaint authorized an apply.
// Useful for logs, events, and metrics labels.
type ApplyReason string

const (
	ApplyReasonNone                = ApplyReason("")
	ApplyReasonPacemakerFenced     = ApplyReason("pacemaker-fenced")
	ApplyReasonNotReadyThreshold   = ApplyReason("notready-threshold")
)

// shouldApplyOutOfServiceTaint returns (true, reason) if the controller should
// apply the out-of-service taint to n, evaluated at "now". The gate combines:
//
//   - Control-plane label present.
//   - Node not cordoned.
//   - Either: Pacemaker reports the node fenced (Online=False, Member=False,
//     FencingHealthy=True in the PacemakerCluster CR), or the node has been
//     NotReady for at least notReadyApplyThreshold.
//
// A nil pacemakerLookup or an unsynced informer disables the Pacemaker fast
// path; the NotReady-duration fallback still applies.
func shouldApplyOutOfServiceTaint(
	n *corev1.Node,
	now time.Time,
	notReadyApplyThreshold time.Duration,
	pacemakerLookup PacemakerLookup,
) (bool, ApplyReason) {
	if !isControlPlaneNode(n) {
		return false, ApplyReasonNone
	}
	if n.Spec.Unschedulable {
		return false, ApplyReasonNone
	}
	readyCond := findCondition(n, corev1.NodeReady)
	if readyCond == nil {
		return false, ApplyReasonNone
	}
	if readyCond.Status == corev1.ConditionTrue {
		return false, ApplyReasonNone
	}

	if pacemakerLookup != nil {
		if pc, ok := pacemakerLookup(); ok && pacemakerReportsNodeFenced(pc, n.Name) {
			return true, ApplyReasonPacemakerFenced
		}
	}

	notReadyFor := now.Sub(readyCond.LastTransitionTime.Time)
	if notReadyFor >= notReadyApplyThreshold {
		return true, ApplyReasonNotReadyThreshold
	}
	return false, ApplyReasonNone
}

// isControlPlaneNode is true when n has either of the standard control-plane
// role labels. Worker nodes have neither and never qualify for the taint.
func isControlPlaneNode(n *corev1.Node) bool {
	if n.Labels == nil {
		return false
	}
	_, hasCP := n.Labels[labelControlPlane]
	_, hasMaster := n.Labels[labelMaster]
	return hasCP || hasMaster
}

// findCondition returns the named NodeCondition from n.Status.Conditions, or
// nil if absent.
func findCondition(n *corev1.Node, t corev1.NodeConditionType) *corev1.NodeCondition {
	for i := range n.Status.Conditions {
		if n.Status.Conditions[i].Type == t {
			return &n.Status.Conditions[i]
		}
	}
	return nil
}

// pacemakerReportsNodeFenced is true when pc has a node entry for nodeName
// whose conditions include Online=False, Member=False, and FencingHealthy=True.
// These are sourced from `pcs status xml` by the pacemaker-status-collector
// CronJob. FencingHealthy=True is required so we do not act on stale data
// where Pacemaker itself is in trouble.
func pacemakerReportsNodeFenced(pc *pacmkrv1.PacemakerCluster, nodeName string) bool {
	if pc == nil || pc.Status.Nodes == nil {
		return false
	}
	for _, ns := range *pc.Status.Nodes {
		if ns.NodeName != nodeName {
			continue
		}
		return conditionIs(ns.Conditions, pacmkrv1.NodeOnlineConditionType, metav1.ConditionFalse) &&
			conditionIs(ns.Conditions, pacmkrv1.NodeMemberConditionType, metav1.ConditionFalse) &&
			conditionIs(ns.Conditions, pacmkrv1.NodeFencingHealthyConditionType, metav1.ConditionTrue)
	}
	return false
}

func conditionIs(conds []metav1.Condition, t string, want metav1.ConditionStatus) bool {
	for i := range conds {
		if conds[i].Type == t {
			return conds[i].Status == want
		}
	}
	return false
}
