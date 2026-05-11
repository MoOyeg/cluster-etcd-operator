// Command tnf-auto-taint-polyfill exercises the cluster-etcd-operator
// auto-taint controller against a live cluster from a developer's machine,
// without rebuilding or redeploying CEO.
//
// It is a thin wrapper around the operator package's RunPolyfill function.
// All of the controller's apply/remove decisions, gate logic, and Pacemaker
// integration are the *same code paths* that run in production CEO; only the
// hosting binary differs.
//
// Usage:
//
//	export KUBECONFIG=/path/to/kubeconfig
//	export TNF_AUTO_OUT_OF_SERVICE_TAINT=true   # required to make it act
//	go run ./cmd/tnf-auto-taint-polyfill
//
// This binary is NOT shipped in the cluster-etcd-operator container image
// (see Dockerfile.ocp — only the production binaries are COPY'd in).
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"

	"github.com/openshift/cluster-etcd-operator/pkg/tnf/operator"
)

func main() {
	klog.InitFlags(nil)
	kubeconfig := flag.String("kubeconfig", os.Getenv("KUBECONFIG"),
		"Path to kubeconfig (defaults to $KUBECONFIG)")
	flag.Parse()

	if *kubeconfig == "" {
		klog.Fatalf("--kubeconfig is required (or set $KUBECONFIG)")
	}

	cfg, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		klog.Fatalf("load kubeconfig from %q: %v", *kubeconfig, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	go func() {
		s := <-sigs
		klog.Infof("received %s, shutting down", s)
		cancel()
	}()

	if err := operator.RunPolyfill(ctx, cfg); err != nil {
		klog.Fatalf("polyfill exited: %v", err)
	}
	klog.Infof("polyfill exited cleanly")
}
