package operator

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestReconcileNodeTaintFeatureGate(t *testing.T) {
	now := time.Now()

	t.Run("disabled does not apply taint", func(t *testing.T) {
		t.Setenv("TNF_AUTO_OUT_OF_SERVICE_TAINT", "")
		node := cpNode("n1", corev1.ConditionFalse, 5*time.Minute)
		client := fake.NewSimpleClientset(node)

		reconcileNodeTaint(context.Background(), client, node, now)

		got, err := client.CoreV1().Nodes().Get(context.Background(), "n1", metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get node: %v", err)
		}
		if hasOutOfServiceTaint(got) {
			t.Fatalf("expected taint to remain absent while feature is disabled, got %+v", got.Spec.Taints)
		}
	})

	t.Run("disabled does not remove taint", func(t *testing.T) {
		t.Setenv("TNF_AUTO_OUT_OF_SERVICE_TAINT", "")
		node := cpNode("n1", corev1.ConditionTrue, 0)
		node.Spec.Taints = []corev1.Taint{oosTaint("nodeshutdown")}
		client := fake.NewSimpleClientset(node)

		reconcileNodeTaint(context.Background(), client, node, now)

		got, err := client.CoreV1().Nodes().Get(context.Background(), "n1", metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get node: %v", err)
		}
		if !hasOutOfServiceTaint(got) {
			t.Fatalf("expected taint to remain present while feature is disabled")
		}
	})

	t.Run("enabled applies taint", func(t *testing.T) {
		t.Setenv("TNF_AUTO_OUT_OF_SERVICE_TAINT", "true")
		node := cpNode("n1", corev1.ConditionFalse, 5*time.Minute)
		client := fake.NewSimpleClientset(node)

		reconcileNodeTaint(context.Background(), client, node, now)

		got, err := client.CoreV1().Nodes().Get(context.Background(), "n1", metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get node: %v", err)
		}
		if !hasOutOfServiceTaint(got) {
			t.Fatalf("expected taint present while feature is enabled, got %+v", got.Spec.Taints)
		}
	})

	t.Run("enabled removes taint from ready node", func(t *testing.T) {
		t.Setenv("TNF_AUTO_OUT_OF_SERVICE_TAINT", "true")
		node := cpNode("n1", corev1.ConditionTrue, 0)
		node.Spec.Taints = []corev1.Taint{oosTaint("nodeshutdown")}
		client := fake.NewSimpleClientset(node)

		reconcileNodeTaint(context.Background(), client, node, now)

		got, err := client.CoreV1().Nodes().Get(context.Background(), "n1", metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get node: %v", err)
		}
		if hasOutOfServiceTaint(got) {
			t.Fatalf("expected taint removed while feature is enabled, got %+v", got.Spec.Taints)
		}
	})
}
