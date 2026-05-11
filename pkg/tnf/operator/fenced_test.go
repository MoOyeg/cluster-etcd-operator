package operator

import (
	"testing"
	"time"

	pacmkrv1 "github.com/openshift/api/etcd/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func cpNode(name string, ready corev1.ConditionStatus, readyAge time.Duration, mutators ...func(*corev1.Node)) *corev1.Node {
	now := time.Now()
	transition := metav1.NewTime(now.Add(-readyAge))
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				labelControlPlane: "",
			},
		},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{
					Type:               corev1.NodeReady,
					Status:             ready,
					LastTransitionTime: transition,
				},
			},
		},
	}
	for _, m := range mutators {
		m(n)
	}
	return n
}

func withWorkerOnly() func(*corev1.Node) {
	return func(n *corev1.Node) {
		delete(n.Labels, labelControlPlane)
		delete(n.Labels, labelMaster)
	}
}
func withCordoned() func(*corev1.Node) {
	return func(n *corev1.Node) { n.Spec.Unschedulable = true }
}
func withNoReadyCondition() func(*corev1.Node) {
	return func(n *corev1.Node) { n.Status.Conditions = nil }
}

func pacemakerOK(nodeName string, online, member, fencingHealthy metav1.ConditionStatus) PacemakerLookup {
	pc := &pacmkrv1.PacemakerCluster{
		Status: pacmkrv1.PacemakerClusterStatus{
			Nodes: &[]pacmkrv1.PacemakerClusterNodeStatus{
				{
					NodeName: nodeName,
					Conditions: []metav1.Condition{
						{Type: pacmkrv1.NodeOnlineConditionType, Status: online},
						{Type: pacmkrv1.NodeMemberConditionType, Status: member},
						{Type: pacmkrv1.NodeFencingHealthyConditionType, Status: fencingHealthy},
					},
				},
			},
		},
	}
	return func() (*pacmkrv1.PacemakerCluster, bool) { return pc, true }
}

func pacemakerEmpty() PacemakerLookup {
	return func() (*pacmkrv1.PacemakerCluster, bool) { return nil, false }
}

func TestShouldApplyOutOfServiceTaint(t *testing.T) {
	const threshold = 3 * time.Minute
	now := time.Now()

	cases := []struct {
		name       string
		node       *corev1.Node
		lookup     PacemakerLookup
		wantApply  bool
		wantReason ApplyReason
	}{
		{
			name:       "Ready control-plane node: never apply",
			node:       cpNode("n", corev1.ConditionTrue, 0),
			lookup:     pacemakerEmpty(),
			wantApply:  false,
			wantReason: ApplyReasonNone,
		},
		{
			name:       "Worker node (no control-plane label): never apply",
			node:       cpNode("n", corev1.ConditionFalse, 10*time.Minute, withWorkerOnly()),
			lookup:     pacemakerEmpty(),
			wantApply:  false,
			wantReason: ApplyReasonNone,
		},
		{
			name:       "Control-plane but cordoned: never apply",
			node:       cpNode("n", corev1.ConditionFalse, 10*time.Minute, withCordoned()),
			lookup:     pacemakerEmpty(),
			wantApply:  false,
			wantReason: ApplyReasonNone,
		},
		{
			name:       "No Ready condition at all: never apply",
			node:       cpNode("n", corev1.ConditionFalse, 10*time.Minute, withNoReadyCondition()),
			lookup:     pacemakerEmpty(),
			wantApply:  false,
			wantReason: ApplyReasonNone,
		},
		{
			name:       "NotReady < threshold, no Pacemaker signal: do not apply",
			node:       cpNode("n", corev1.ConditionFalse, 30*time.Second),
			lookup:     pacemakerEmpty(),
			wantApply:  false,
			wantReason: ApplyReasonNone,
		},
		{
			name:       "NotReady >= threshold, no Pacemaker signal: apply via threshold",
			node:       cpNode("n", corev1.ConditionFalse, 5*time.Minute),
			lookup:     pacemakerEmpty(),
			wantApply:  true,
			wantReason: ApplyReasonNotReadyThreshold,
		},
		{
			name: "Unknown < threshold, no Pacemaker signal: do not apply",
			node: cpNode("n", corev1.ConditionUnknown, 30*time.Second),
			lookup: pacemakerEmpty(),
			wantApply:  false,
			wantReason: ApplyReasonNone,
		},
		{
			name: "Pacemaker confirms fenced (< threshold): apply via Pacemaker fast path",
			node: cpNode("n", corev1.ConditionUnknown, 5*time.Second),
			lookup: pacemakerOK("n",
				metav1.ConditionFalse, // Online=False
				metav1.ConditionFalse, // Member=False
				metav1.ConditionTrue,  // FencingHealthy=True
			),
			wantApply:  true,
			wantReason: ApplyReasonPacemakerFenced,
		},
		{
			name: "Pacemaker says Online (not fenced) yet K8s says NotReady < threshold: do not apply",
			node: cpNode("n", corev1.ConditionFalse, 5*time.Second),
			lookup: pacemakerOK("n",
				metav1.ConditionTrue,  // Online=True (NOT fenced)
				metav1.ConditionTrue,  // Member=True
				metav1.ConditionTrue,  // FencingHealthy=True
			),
			wantApply:  false,
			wantReason: ApplyReasonNone,
		},
		{
			name: "Pacemaker has the node but FencingHealthy=False: ignore Pacemaker, fall through to threshold",
			node: cpNode("n", corev1.ConditionFalse, 30*time.Second),
			lookup: pacemakerOK("n",
				metav1.ConditionFalse,
				metav1.ConditionFalse,
				metav1.ConditionFalse, // FencingHealthy=False ⇒ untrusted
			),
			wantApply:  false,
			wantReason: ApplyReasonNone,
		},
		{
			name: "Pacemaker FencingHealthy=False but threshold elapsed: still apply via threshold",
			node: cpNode("n", corev1.ConditionFalse, 5*time.Minute),
			lookup: pacemakerOK("n",
				metav1.ConditionFalse,
				metav1.ConditionFalse,
				metav1.ConditionFalse,
			),
			wantApply:  true,
			wantReason: ApplyReasonNotReadyThreshold,
		},
		{
			name:       "Nil pacemakerLookup, NotReady < threshold: do not apply",
			node:       cpNode("n", corev1.ConditionFalse, 30*time.Second),
			lookup:     nil,
			wantApply:  false,
			wantReason: ApplyReasonNone,
		},
		{
			name:       "Nil pacemakerLookup, NotReady >= threshold: apply via threshold",
			node:       cpNode("n", corev1.ConditionFalse, 5*time.Minute),
			lookup:     nil,
			wantApply:  true,
			wantReason: ApplyReasonNotReadyThreshold,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotApply, gotReason := shouldApplyOutOfServiceTaint(c.node, now, threshold, c.lookup)
			if gotApply != c.wantApply || gotReason != c.wantReason {
				t.Fatalf("shouldApplyOutOfServiceTaint = (%v, %q), want (%v, %q)",
					gotApply, gotReason, c.wantApply, c.wantReason)
			}
		})
	}
}

func TestIsControlPlaneNode(t *testing.T) {
	cases := []struct {
		name   string
		labels map[string]string
		want   bool
	}{
		{"no labels", nil, false},
		{"control-plane only", map[string]string{labelControlPlane: ""}, true},
		{"legacy master only", map[string]string{labelMaster: ""}, true},
		{"both labels", map[string]string{labelControlPlane: "", labelMaster: ""}, true},
		{"worker labels", map[string]string{"node-role.kubernetes.io/worker": ""}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			n := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Labels: c.labels}}
			if got := isControlPlaneNode(n); got != c.want {
				t.Fatalf("isControlPlaneNode = %v, want %v", got, c.want)
			}
		})
	}
}

func TestPacemakerReportsNodeFenced(t *testing.T) {
	mkPC := func(nodeName string, conds ...metav1.Condition) *pacmkrv1.PacemakerCluster {
		return &pacmkrv1.PacemakerCluster{
			Status: pacmkrv1.PacemakerClusterStatus{
				Nodes: &[]pacmkrv1.PacemakerClusterNodeStatus{
					{NodeName: nodeName, Conditions: conds},
				},
			},
		}
	}
	cond := func(t string, s metav1.ConditionStatus) metav1.Condition {
		return metav1.Condition{Type: t, Status: s}
	}

	cases := []struct {
		name string
		pc   *pacmkrv1.PacemakerCluster
		node string
		want bool
	}{
		{"nil PacemakerCluster", nil, "n", false},
		{"empty nodes", &pacmkrv1.PacemakerCluster{}, "n", false},
		{
			"node entry absent",
			mkPC("other",
				cond(pacmkrv1.NodeOnlineConditionType, metav1.ConditionFalse),
				cond(pacmkrv1.NodeMemberConditionType, metav1.ConditionFalse),
				cond(pacmkrv1.NodeFencingHealthyConditionType, metav1.ConditionTrue),
			),
			"n",
			false,
		},
		{
			"all three conditions correct: fenced",
			mkPC("n",
				cond(pacmkrv1.NodeOnlineConditionType, metav1.ConditionFalse),
				cond(pacmkrv1.NodeMemberConditionType, metav1.ConditionFalse),
				cond(pacmkrv1.NodeFencingHealthyConditionType, metav1.ConditionTrue),
			),
			"n",
			true,
		},
		{
			"Online=True (not fenced)",
			mkPC("n",
				cond(pacmkrv1.NodeOnlineConditionType, metav1.ConditionTrue),
				cond(pacmkrv1.NodeMemberConditionType, metav1.ConditionFalse),
				cond(pacmkrv1.NodeFencingHealthyConditionType, metav1.ConditionTrue),
			),
			"n",
			false,
		},
		{
			"FencingHealthy=False (untrusted)",
			mkPC("n",
				cond(pacmkrv1.NodeOnlineConditionType, metav1.ConditionFalse),
				cond(pacmkrv1.NodeMemberConditionType, metav1.ConditionFalse),
				cond(pacmkrv1.NodeFencingHealthyConditionType, metav1.ConditionFalse),
			),
			"n",
			false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := pacemakerReportsNodeFenced(c.pc, c.node); got != c.want {
				t.Fatalf("pacemakerReportsNodeFenced = %v, want %v", got, c.want)
			}
		})
	}
}
