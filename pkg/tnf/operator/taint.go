package operator

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
)

// Constants for the out-of-service taint lifecycle managed by this controller.
// See enhancements/two-node-fencing/auto-out-of-service-taint.md.
const (
	// OutOfServiceTaintKey is the taint key recognized by Kubernetes Pod GC and the
	// CSI external-attacher. When applied with effect NoExecute, Pod GC force-deletes
	// pods that do not tolerate it and CSI releases their VolumeAttachments without
	// waiting for kubelet on the dead node.
	OutOfServiceTaintKey = "node.kubernetes.io/out-of-service"

	// OutOfServiceTaintValue is the value this controller writes when applying the
	// taint. The upstream feature gate ignores the value (only key + effect are
	// inspected), so this controller treats the value as opaque on read.
	OutOfServiceTaintValue = "nodeshutdown"

	// FieldManagerTNFTaint is the FieldManager attributed to Node patches made by
	// the auto-taint controller, for audit and conflict reporting via
	// --show-managed-fields.
	FieldManagerTNFTaint = "cluster-etcd-operator/tnf-taint"
)

// hasOutOfServiceTaint reports whether n carries any taint with the
// out-of-service key. Effect and value are ignored on read so taints written by
// admins (with the historical "node-fenced" value) or by other tooling are still
// recognized.
func hasOutOfServiceTaint(n *corev1.Node) bool {
	for i := range n.Spec.Taints {
		if n.Spec.Taints[i].Key == OutOfServiceTaintKey {
			return true
		}
	}
	return false
}

// applyOutOfServiceTaint ensures the out-of-service taint is present on the
// named Node. Idempotent: a no-op if the taint is already present. Uses
// RetryOnConflict so it composes with concurrent admin or controller writes
// to spec.taints. Returns nil if the Node has been deleted in the meantime.
func applyOutOfServiceTaint(ctx context.Context, kc kubernetes.Interface, nodeName string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		n, err := kc.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if hasOutOfServiceTaint(n) {
			return nil
		}
		n.Spec.Taints = append(n.Spec.Taints, corev1.Taint{
			Key:    OutOfServiceTaintKey,
			Value:  OutOfServiceTaintValue,
			Effect: corev1.TaintEffectNoExecute,
		})
		_, err = kc.CoreV1().Nodes().Update(ctx, n, metav1.UpdateOptions{
			FieldManager: FieldManagerTNFTaint,
		})
		return err
	})
}

// removeOutOfServiceTaint ensures no out-of-service-keyed taint is present on
// the named Node. Idempotent: a no-op if no such taint is set. Matches by key
// only so taints with the historical "node-fenced" value (or any other) are
// cleaned up too. Returns nil if the Node has been deleted in the meantime.
func removeOutOfServiceTaint(ctx context.Context, kc kubernetes.Interface, nodeName string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		n, err := kc.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		kept := make([]corev1.Taint, 0, len(n.Spec.Taints))
		found := false
		for _, t := range n.Spec.Taints {
			if t.Key == OutOfServiceTaintKey {
				found = true
				continue
			}
			kept = append(kept, t)
		}
		if !found {
			return nil
		}
		n.Spec.Taints = kept
		_, err = kc.CoreV1().Nodes().Update(ctx, n, metav1.UpdateOptions{
			FieldManager: FieldManagerTNFTaint,
		})
		return err
	})
}
