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

// csi-check drives the node service of a running daos-csi without Kubernetes:
// NodeGetInfo -> NodeStageVolume -> NodePublishVolume -> write/read a file ->
// NodeUnpublishVolume -> NodeUnstageVolume. Used on a test bed to prove that
// dfuse inside the node image mounts a real DAOS container over the fabric.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	endpoint := flag.String("endpoint", "unix:///csi/csi.sock", "daos-csi endpoint")
	pool := flag.String("pool", "", "pool label or UUID")
	cont := flag.String("container", "", "container label or UUID")
	volume := flag.String("volume", "csi-check", "volume id")
	staging := flag.String("staging", "/tmp/csi-check/staging", "staging target path")
	target := flag.String("target", "/tmp/csi-check/target", "publish target path")
	keep := flag.Bool("keep", false, "leave the volume staged/published")
	flag.Parse()
	if *pool == "" || *cont == "" {
		fmt.Fprintln(os.Stderr, "--pool and --container are required")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	conn, err := grpc.NewClient(*endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fail("dial", err)
	}
	node := csi.NewNodeClient(conn)
	ident := csi.NewIdentityClient(conn)

	info, err := ident.GetPluginInfo(ctx, &csi.GetPluginInfoRequest{})
	if err != nil {
		fail("GetPluginInfo", err)
	}
	ni, err := node.NodeGetInfo(ctx, &csi.NodeGetInfoRequest{})
	if err != nil {
		fail("NodeGetInfo", err)
	}
	fmt.Printf("plugin %s %s, node %s\n", info.GetName(), info.GetVendorVersion(), ni.GetNodeId())

	capability := &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER}}
	vc := map[string]string{"pool": *pool, "container": *cont}
	step("NodeStageVolume", func() error {
		_, err := node.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{VolumeId: *volume, StagingTargetPath: *staging, VolumeCapability: capability, VolumeContext: vc})
		return err
	})
	step("NodePublishVolume", func() error {
		_, err := node.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{VolumeId: *volume, StagingTargetPath: *staging, TargetPath: *target, VolumeCapability: capability, VolumeContext: vc})
		return err
	})
	step("write/read through the mount", func() error {
		p := filepath.Join(*target, fmt.Sprintf("csi-check-%d.txt", time.Now().Unix()))
		want := []byte("hello from daos-csi " + time.Now().Format(time.RFC3339) + "\n")
		if err := os.WriteFile(p, want, 0o644); err != nil {
			return err
		}
		got, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		if string(got) != string(want) {
			return fmt.Errorf("read back mismatch")
		}
		entries, _ := os.ReadDir(*target)
		fmt.Printf("  wrote %s; %d entr(y/ies) in %s\n", filepath.Base(p), len(entries), *target)
		return os.Remove(p)
	})
	if *keep {
		fmt.Println("kept (--keep)")
		return
	}
	step("NodeUnpublishVolume", func() error {
		_, err := node.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{VolumeId: *volume, TargetPath: *target})
		return err
	})
	step("NodeUnstageVolume", func() error {
		_, err := node.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{VolumeId: *volume, StagingTargetPath: *staging})
		return err
	})
	fmt.Println("ALL PASS")
}

func step(name string, f func() error) {
	start := time.Now()
	if err := f(); err != nil {
		fail(name, err)
	}
	fmt.Printf("ok  %-28s %s\n", name, time.Since(start).Round(time.Millisecond))
}

func fail(what string, err error) {
	fmt.Fprintf(os.Stderr, "FAIL %s: %v\n", what, err)
	os.Exit(1)
}
