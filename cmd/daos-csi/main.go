/*
SPDX-License-Identifier: Apache-2.0
Copyright 2026 Gluesys Co., Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// daos-csi is the CSI driver binary; --mode controller runs next to
// csi-provisioner in a Deployment, --mode node runs in the per-node
// DaemonSet next to csi-node-driver-registrar and a daos_agent sidecar.
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"

	"gitlab.gluesys.com/exastor/daos-csi/pkg/driver"
)

func main() {
	klog.InitFlags(nil)
	var (
		endpoint   = flag.String("endpoint", "unix:///csi/csi.sock", "CSI endpoint")
		mode       = flag.String("mode", "all", "controller | node | all")
		nodeID     = flag.String("node-id", os.Getenv("NODE_ID"), "node name (node mode)")
		name       = flag.String("driver-name", driver.DefaultDriverName, "CSI driver name")
		namespace  = flag.String("namespace", os.Getenv("DAOS_CSI_NAMESPACE"), "namespace for DaosContainer CRs (controller mode)")
		kubeconfig = flag.String("kubeconfig", os.Getenv("KUBECONFIG"), "kubeconfig (empty = in-cluster)")
		stagingDir = flag.String("staging-dir", "/var/lib/daos-csi", "host dir for dfuse mounts and state (node mode)")
		dfuseArgs  = flag.String("dfuse-args", "", "extra dfuse flags, comma separated (e.g. --disable-caching)")
	)
	flag.Parse()
	cfg := driver.Config{Name: *name, NodeID: *nodeID, Mode: driver.Mode(*mode), Endpoint: *endpoint, Namespace: *namespace, StagingDir: *stagingDir}
	if cfg.Mode != driver.ModeNode {
		rc, err := restConfig(*kubeconfig)
		if err != nil {
			klog.Fatalf("kubernetes config: %v", err)
		}
		dyn, err := dynamic.NewForConfig(rc)
		if err != nil {
			klog.Fatalf("dynamic client: %v", err)
		}
		cfg.Containers = &driver.DynamicClient{Dyn: dyn}
		if cfg.Namespace == "" {
			klog.Fatal("--namespace (or DAOS_CSI_NAMESPACE) is required in controller mode")
		}
	}
	if cfg.Mode != driver.ModeController {
		m := driver.NewSystemMounter()
		var extra []string
		if *dfuseArgs != "" {
			extra = strings.Split(*dfuseArgs, ",")
		}
		cfg.Mounter = m
		cfg.Fuse = driver.NewDfuseRunner(m, extra...)
	}
	d, err := driver.New(cfg)
	if err != nil {
		klog.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := d.Run(ctx); err != nil {
		klog.Fatal(err)
	}
}

func restConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig == "" {
		if c, err := rest.InClusterConfig(); err == nil {
			return c, nil
		}
	}
	return clientcmd.BuildConfigFromFlags("", kubeconfig)
}
