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

func TestConfiguredNotReadyApplyThreshold(t *testing.T) {
	t.Run("uses default when unset", func(t *testing.T) {
		t.Setenv(notReadyApplyThresholdEnvVar, "")
		if got := configuredNotReadyApplyThreshold(); got != notReadyApplyThreshold {
			t.Fatalf("expected default threshold %s, got %s", notReadyApplyThreshold, got)
		}
	})

	t.Run("uses positive whole seconds from env", func(t *testing.T) {
		t.Setenv(notReadyApplyThresholdEnvVar, "45")
		if got := configuredNotReadyApplyThreshold(); got != 45*time.Second {
			t.Fatalf("expected 45s threshold, got %s", got)
		}
	})

	t.Run("falls back on invalid values", func(t *testing.T) {
		for _, tc := range []string{"0", "-1", "forty-five"} {
			t.Run(tc, func(t *testing.T) {
				t.Setenv(notReadyApplyThresholdEnvVar, tc)
				if got := configuredNotReadyApplyThreshold(); got != notReadyApplyThreshold {
					t.Fatalf("expected default threshold %s for %q, got %s", notReadyApplyThreshold, tc, got)
				}
			})
		}
	})
}
