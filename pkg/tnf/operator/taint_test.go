package operator

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func nodeWithTaints(name string, taints ...corev1.Taint) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       corev1.NodeSpec{Taints: taints},
	}
}

func oosTaint(value string) corev1.Taint {
	return corev1.Taint{
		Key:    OutOfServiceTaintKey,
		Value:  value,
		Effect: corev1.TaintEffectNoExecute,
	}
}

func TestHasOutOfServiceTaint(t *testing.T) {
	cases := []struct {
		name string
		node *corev1.Node
		want bool
	}{
		{
			name: "no taints",
			node: nodeWithTaints("n"),
			want: false,
		},
		{
			name: "has out-of-service taint with nodeshutdown value",
			node: nodeWithTaints("n", oosTaint("nodeshutdown")),
			want: true,
		},
		{
			name: "has out-of-service taint with legacy node-fenced value",
			node: nodeWithTaints("n", oosTaint("node-fenced")),
			want: true,
		},
		{
			name: "has out-of-service taint with empty value",
			node: nodeWithTaints("n", oosTaint("")),
			want: true,
		},
		{
			name: "has unrelated taints only",
			node: nodeWithTaints("n",
				corev1.Taint{Key: "node.kubernetes.io/unreachable", Effect: corev1.TaintEffectNoExecute},
				corev1.Taint{Key: "node.kubernetes.io/not-ready", Effect: corev1.TaintEffectNoExecute},
			),
			want: false,
		},
		{
			name: "out-of-service taint among many",
			node: nodeWithTaints("n",
				corev1.Taint{Key: "node.kubernetes.io/unreachable", Effect: corev1.TaintEffectNoExecute},
				oosTaint("nodeshutdown"),
				corev1.Taint{Key: "custom/foo", Effect: corev1.TaintEffectNoSchedule},
			),
			want: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := hasOutOfServiceTaint(c.node)
			if got != c.want {
				t.Fatalf("hasOutOfServiceTaint() = %v, want %v", got, c.want)
			}
		})
	}
}

func TestApplyOutOfServiceTaint(t *testing.T) {
	t.Run("adds taint when absent", func(t *testing.T) {
		node := nodeWithTaints("n1")
		client := fake.NewSimpleClientset(node)
		if err := applyOutOfServiceTaint(context.Background(), client, "n1"); err != nil {
			t.Fatalf("apply returned err: %v", err)
		}
		got, _ := client.CoreV1().Nodes().Get(context.Background(), "n1", metav1.GetOptions{})
		if !hasOutOfServiceTaint(got) {
			t.Fatalf("expected taint present, got taints=%+v", got.Spec.Taints)
		}
		// Verify exact key/value/effect.
		var found *corev1.Taint
		for i := range got.Spec.Taints {
			if got.Spec.Taints[i].Key == OutOfServiceTaintKey {
				found = &got.Spec.Taints[i]
				break
			}
		}
		if found == nil || found.Value != OutOfServiceTaintValue || found.Effect != corev1.TaintEffectNoExecute {
			t.Fatalf("applied taint shape wrong: %+v", found)
		}
	})

	t.Run("idempotent when taint already present", func(t *testing.T) {
		node := nodeWithTaints("n1", oosTaint("nodeshutdown"))
		client := fake.NewSimpleClientset(node)

		var updates int
		client.PrependReactor("update", "nodes", func(action clienttesting.Action) (bool, runtime.Object, error) {
			updates++
			return false, nil, nil
		})

		if err := applyOutOfServiceTaint(context.Background(), client, "n1"); err != nil {
			t.Fatalf("apply returned err: %v", err)
		}
		if updates != 0 {
			t.Fatalf("expected no Update calls when taint already present, got %d", updates)
		}
	})

	t.Run("idempotent on legacy node-fenced value (matches by key only)", func(t *testing.T) {
		// Even though this controller writes "nodeshutdown", an existing taint
		// with the historical "node-fenced" value is treated as already-applied.
		node := nodeWithTaints("n1", oosTaint("node-fenced"))
		client := fake.NewSimpleClientset(node)

		var updates int
		client.PrependReactor("update", "nodes", func(action clienttesting.Action) (bool, runtime.Object, error) {
			updates++
			return false, nil, nil
		})

		if err := applyOutOfServiceTaint(context.Background(), client, "n1"); err != nil {
			t.Fatalf("apply returned err: %v", err)
		}
		if updates != 0 {
			t.Fatalf("expected no Update on legacy taint, got %d", updates)
		}
	})

	t.Run("nil on NotFound", func(t *testing.T) {
		client := fake.NewSimpleClientset()
		if err := applyOutOfServiceTaint(context.Background(), client, "ghost"); err != nil {
			t.Fatalf("expected nil on NotFound, got %v", err)
		}
	})

	t.Run("preserves unrelated taints", func(t *testing.T) {
		other := corev1.Taint{Key: "custom/foo", Effect: corev1.TaintEffectNoSchedule}
		node := nodeWithTaints("n1", other)
		client := fake.NewSimpleClientset(node)
		if err := applyOutOfServiceTaint(context.Background(), client, "n1"); err != nil {
			t.Fatalf("apply: %v", err)
		}
		got, _ := client.CoreV1().Nodes().Get(context.Background(), "n1", metav1.GetOptions{})
		if len(got.Spec.Taints) != 2 {
			t.Fatalf("expected 2 taints after apply (unrelated + out-of-service), got %d: %+v",
				len(got.Spec.Taints), got.Spec.Taints)
		}
	})

	t.Run("retries on conflict and succeeds", func(t *testing.T) {
		node := nodeWithTaints("n1")
		client := fake.NewSimpleClientset(node)

		var attempts int
		gr := schema.GroupResource{Group: "", Resource: "nodes"}
		client.PrependReactor("update", "nodes", func(action clienttesting.Action) (bool, runtime.Object, error) {
			attempts++
			if attempts == 1 {
				return true, nil, apierrors.NewConflict(gr, "n1", nil)
			}
			return false, nil, nil
		})

		if err := applyOutOfServiceTaint(context.Background(), client, "n1"); err != nil {
			t.Fatalf("apply (with retry): %v", err)
		}
		if attempts < 2 {
			t.Fatalf("expected at least 2 update attempts under conflict, got %d", attempts)
		}
		got, _ := client.CoreV1().Nodes().Get(context.Background(), "n1", metav1.GetOptions{})
		if !hasOutOfServiceTaint(got) {
			t.Fatalf("expected taint after retry, got %+v", got.Spec.Taints)
		}
	})
}

func TestRemoveOutOfServiceTaint(t *testing.T) {
	t.Run("removes when present", func(t *testing.T) {
		node := nodeWithTaints("n1", oosTaint("nodeshutdown"))
		client := fake.NewSimpleClientset(node)
		if err := removeOutOfServiceTaint(context.Background(), client, "n1"); err != nil {
			t.Fatalf("remove: %v", err)
		}
		got, _ := client.CoreV1().Nodes().Get(context.Background(), "n1", metav1.GetOptions{})
		if hasOutOfServiceTaint(got) {
			t.Fatalf("expected taint absent, got %+v", got.Spec.Taints)
		}
	})

	t.Run("removes legacy node-fenced value (key-only match)", func(t *testing.T) {
		node := nodeWithTaints("n1", oosTaint("node-fenced"))
		client := fake.NewSimpleClientset(node)
		if err := removeOutOfServiceTaint(context.Background(), client, "n1"); err != nil {
			t.Fatalf("remove: %v", err)
		}
		got, _ := client.CoreV1().Nodes().Get(context.Background(), "n1", metav1.GetOptions{})
		if hasOutOfServiceTaint(got) {
			t.Fatalf("expected legacy taint removed, got %+v", got.Spec.Taints)
		}
	})

	t.Run("idempotent when absent (no API write)", func(t *testing.T) {
		node := nodeWithTaints("n1")
		client := fake.NewSimpleClientset(node)

		var updates int
		client.PrependReactor("update", "nodes", func(action clienttesting.Action) (bool, runtime.Object, error) {
			updates++
			return false, nil, nil
		})

		if err := removeOutOfServiceTaint(context.Background(), client, "n1"); err != nil {
			t.Fatalf("remove on clean node: %v", err)
		}
		if updates != 0 {
			t.Fatalf("expected no Update when taint absent, got %d", updates)
		}
	})

	t.Run("preserves unrelated taints", func(t *testing.T) {
		other := corev1.Taint{Key: "custom/foo", Effect: corev1.TaintEffectNoSchedule}
		node := nodeWithTaints("n1", oosTaint("nodeshutdown"), other)
		client := fake.NewSimpleClientset(node)
		if err := removeOutOfServiceTaint(context.Background(), client, "n1"); err != nil {
			t.Fatalf("remove: %v", err)
		}
		got, _ := client.CoreV1().Nodes().Get(context.Background(), "n1", metav1.GetOptions{})
		if len(got.Spec.Taints) != 1 || got.Spec.Taints[0] != other {
			t.Fatalf("expected only unrelated taint to remain, got %+v", got.Spec.Taints)
		}
	})

	t.Run("nil on NotFound", func(t *testing.T) {
		client := fake.NewSimpleClientset()
		if err := removeOutOfServiceTaint(context.Background(), client, "ghost"); err != nil {
			t.Fatalf("expected nil on NotFound, got %v", err)
		}
	})

	t.Run("retries on conflict", func(t *testing.T) {
		node := nodeWithTaints("n1", oosTaint("nodeshutdown"))
		client := fake.NewSimpleClientset(node)

		var attempts int
		gr := schema.GroupResource{Group: "", Resource: "nodes"}
		client.PrependReactor("update", "nodes", func(action clienttesting.Action) (bool, runtime.Object, error) {
			attempts++
			if attempts == 1 {
				return true, nil, apierrors.NewConflict(gr, "n1", nil)
			}
			return false, nil, nil
		})

		if err := removeOutOfServiceTaint(context.Background(), client, "n1"); err != nil {
			t.Fatalf("remove (with retry): %v", err)
		}
		if attempts < 2 {
			t.Fatalf("expected at least 2 update attempts under conflict, got %d", attempts)
		}
	})
}
