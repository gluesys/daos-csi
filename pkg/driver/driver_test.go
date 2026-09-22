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

package driver

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/csi-test/v5/pkg/sanity"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func rwx() *csi.VolumeCapability {
	return &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER}}
}

func newTestDriver(t *testing.T, crs *fakeCRs, fuse *fakeFuse, mounts *fakeMounts) *Driver {
	d, err := New(Config{Endpoint: "unix://" + filepath.Join(t.TempDir(), "csi.sock"), NodeID: "node-a", Containers: crs, Namespace: "daos-csi",
		Mounter: mounts, Fuse: fuse, StagingDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestCreateVolumeWritesDaosContainerFromStorageClass(t *testing.T) {
	ctx := context.Background()
	crs := newFakeCRs()
	d := newTestDriver(t, crs, newFakeFuse(), newFakeMounts())
	resp, err := d.CreateVolume(ctx, &csi.CreateVolumeRequest{Name: "pvc-1", VolumeCapabilities: []*csi.VolumeCapability{rwx()},
		CapacityRange: &csi.CapacityRange{RequiredBytes: 10 << 30},
		Parameters:    map[string]string{ParamPool: "kv", ParamOclass: "RP_2GX", ParamDirOclass: "RP_2G1", ParamChunkSize: "4194304", ParamRdFac: "1", ParamCsum: "crc32", "property.ec_cell_sz": "131072"}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Volume.VolumeId != "pvc-1" || resp.Volume.CapacityBytes != 10<<30 {
		t.Fatalf("%+v", resp.Volume)
	}
	vc := resp.Volume.VolumeContext
	if vc[ctxPool] != "kv" || vc[ctxContainer] != "pvc-1" || vc[ctxPoolUUID] != "pool-uuid-1" || vc[ctxContUUID] != "cont-uuid-pvc-1" || vc[ctxNamespace] != "daos-csi" {
		t.Fatalf("volume context: %v", vc)
	}
	spec := crs.containers["daos-csi/pvc-1"]
	if spec.PoolRef != "kv" || spec.FileOclass != "RP_2GX" || spec.DirOclass != "RP_2G1" || spec.ChunkSize != 4194304 || *spec.RedundancyFac != 1 || spec.Checksum != "crc32" || spec.Properties["ec_cell_sz"] != "131072" || spec.Type != "POSIX" {
		t.Fatalf("spec: %+v", spec)
	}
	// idempotent
	resp2, err := d.CreateVolume(ctx, &csi.CreateVolumeRequest{Name: "pvc-1", VolumeCapabilities: []*csi.VolumeCapability{rwx()}, Parameters: map[string]string{ParamPool: "kv"}})
	if err != nil || resp2.Volume.VolumeId != "pvc-1" || len(crs.containers) != 1 {
		t.Fatalf("second create: %v %d", err, len(crs.containers))
	}
	// different size -> AlreadyExists
	_, err = d.CreateVolume(ctx, &csi.CreateVolumeRequest{Name: "pvc-1", VolumeCapabilities: []*csi.VolumeCapability{rwx()}, Parameters: map[string]string{ParamPool: "kv"},
		CapacityRange: &csi.CapacityRange{RequiredBytes: 20 << 30}})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("size mismatch: %v", err)
	}
	long := strings.Repeat("a", 200)
	if l := containerLabel(long); len(l) != 127 || !strings.HasPrefix(l, "aaaa") {
		t.Fatalf("label shortening: %d %q", len(l), l)
	}
}

func TestCreateVolumeErrors(t *testing.T) {
	ctx := context.Background()
	crs := newFakeCRs()
	d := newTestDriver(t, crs, newFakeFuse(), newFakeMounts())
	cases := []struct {
		name string
		req  *csi.CreateVolumeRequest
		code codes.Code
	}{
		{"no pool param", &csi.CreateVolumeRequest{Name: "v", VolumeCapabilities: []*csi.VolumeCapability{rwx()}}, codes.InvalidArgument},
		{"unknown pool", &csi.CreateVolumeRequest{Name: "v", VolumeCapabilities: []*csi.VolumeCapability{rwx()}, Parameters: map[string]string{ParamPool: "nope"}}, codes.NotFound},
		{"bad chunk", &csi.CreateVolumeRequest{Name: "v", VolumeCapabilities: []*csi.VolumeCapability{rwx()}, Parameters: map[string]string{ParamPool: "kv", ParamChunkSize: "big"}}, codes.InvalidArgument},
		{"block", &csi.CreateVolumeRequest{Name: "v", VolumeCapabilities: []*csi.VolumeCapability{{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER}}}, Parameters: map[string]string{ParamPool: "kv"}}, codes.InvalidArgument},
		{"too big for pool", &csi.CreateVolumeRequest{Name: "v", VolumeCapabilities: []*csi.VolumeCapability{rwx()}, Parameters: map[string]string{ParamPool: "kv"},
			CapacityRange: &csi.CapacityRange{RequiredBytes: 200 << 30}}, codes.OutOfRange},
	}
	for _, c := range cases {
		_, err := d.CreateVolume(ctx, c.req)
		if status.Code(err) != c.code {
			t.Errorf("%s: got %v want %v", c.name, err, c.code)
		}
	}
	// pool without uuid yet -> Unavailable (provisioner retries)
	crs.pools["new"] = &PoolStatus{Exists: true, Label: "new"}
	_, err := d.CreateVolume(ctx, &csi.CreateVolumeRequest{Name: "v", VolumeCapabilities: []*csi.VolumeCapability{rwx()}, Parameters: map[string]string{ParamPool: "new"}})
	if status.Code(err) != codes.Unavailable {
		t.Errorf("pool not ready: %v", err)
	}
	// operator reports a failure -> Internal with the reason
	crs.failReason = "CreateFailed"
	_, err = d.CreateVolume(ctx, &csi.CreateVolumeRequest{Name: "v2", VolumeCapabilities: []*csi.VolumeCapability{rwx()}, Parameters: map[string]string{ParamPool: "kv"}})
	if status.Code(err) != codes.Internal {
		t.Errorf("failed create: %v", err)
	}
}

func TestDeleteVolumeDestroyFlag(t *testing.T) {
	ctx := context.Background()
	crs := newFakeCRs()
	d := newTestDriver(t, crs, newFakeFuse(), newFakeMounts())
	_, _ = d.CreateVolume(ctx, &csi.CreateVolumeRequest{Name: "pvc-1", VolumeCapabilities: []*csi.VolumeCapability{rwx()}, Parameters: map[string]string{ParamPool: "kv"}})
	if _, err := d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "pvc-1"}); err != nil {
		t.Fatal(err)
	}
	if !crs.destroyed["daos-csi/pvc-1"] {
		t.Fatal("default delete must approve destruction (reclaimPolicy Delete)")
	}
	_, _ = d.CreateVolume(ctx, &csi.CreateVolumeRequest{Name: "pvc-2", VolumeCapabilities: []*csi.VolumeCapability{rwx()}, Parameters: map[string]string{ParamPool: "kv"}})
	if _, err := d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "pvc-2", Secrets: map[string]string{ParamDestroyOnDele: "false"}}); err != nil {
		t.Fatal(err)
	}
	if crs.destroyed["daos-csi/pvc-2"] {
		t.Fatal("destroyOnDelete=false must orphan the DAOS container")
	}
	// deleting an unknown volume is fine (idempotent)
	if _, err := d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "nope"}); err != nil {
		t.Fatal(err)
	}
}

func TestNodeStagePublishAndRecovery(t *testing.T) {
	ctx := context.Background()
	fuse, mounts := newFakeFuse(), newFakeMounts()
	d := newTestDriver(t, newFakeCRs(), fuse, mounts)
	staging, target := filepath.Join(t.TempDir(), "staging"), filepath.Join(t.TempDir(), "target")
	vc := map[string]string{ctxPool: "kv", ctxContainer: "pvc-1", ctxPoolUUID: "pu", ctxContUUID: "cu"}
	if _, err := d.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{VolumeId: "pvc-1", StagingTargetPath: staging, VolumeCapability: rwx(), VolumeContext: vc}); err != nil {
		t.Fatal(err)
	}
	mnt := d.stagePath("pvc-1")
	if got := fuse.running[mnt]; got != [2]string{"pu", "cu"} {
		t.Fatalf("dfuse must use UUIDs when present: %v", got)
	}
	if mounts.mounts[staging] != mnt {
		t.Fatalf("staging path must be a bind of the dfuse mount: %v", mounts.mounts)
	}
	if _, err := d.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{VolumeId: "pvc-1", StagingTargetPath: staging, TargetPath: target, VolumeCapability: rwx(), Readonly: true}); err != nil {
		t.Fatal(err)
	}
	if mounts.mounts[target] != mnt {
		t.Fatalf("target must bind the dfuse mount: %v", mounts.mounts)
	}
	// idempotent stage/publish
	if _, err := d.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{VolumeId: "pvc-1", StagingTargetPath: staging, VolumeCapability: rwx(), VolumeContext: vc}); err != nil || fuse.starts != 1 {
		t.Fatalf("re-stage: %v starts=%d", err, fuse.starts)
	}

	// plugin restart: a new Driver with the same StagingDir and an empty fuse recovers the mount
	fuse2 := newFakeFuse()
	d2, _ := New(Config{Endpoint: "unix:///tmp/x.sock", NodeID: "node-a", Containers: newFakeCRs(), Namespace: "n", Mounter: mounts, Fuse: fuse2, StagingDir: d.cfg.StagingDir})
	if err := d2.recoverMounts(ctx); err != nil {
		t.Fatal(err)
	}
	if !fuse2.Running(mnt) {
		t.Fatal("state file must bring dfuse back after a restart")
	}

	if _, err := d.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{VolumeId: "pvc-1", TargetPath: target}); err != nil {
		t.Fatal(err)
	}
	if _, err := d.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{VolumeId: "pvc-1", StagingTargetPath: staging}); err != nil {
		t.Fatal(err)
	}
	if fuse.Running(mnt) || len(mounts.mounts) != 0 {
		t.Fatalf("unstage must stop dfuse and unmount everything: %v %v", fuse.running, mounts.mounts)
	}
	if _, err := d.loadState("pvc-1"); err == nil {
		t.Fatal("state must be removed on unstage")
	}
	// publish without stage
	if _, err := d.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{VolumeId: "pvc-9", StagingTargetPath: staging, TargetPath: target, VolumeCapability: rwx()}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unstaged publish: %v", err)
	}
}

func TestUnstageIsIdempotentWhenTheMountIsGone(t *testing.T) {
	ctx := context.Background()
	d := newTestDriver(t, newFakeCRs(), newFakeFuse(), newFakeMounts())
	staging := filepath.Join(t.TempDir(), "staging")
	// never staged on this node: unstage must succeed, not fail forever
	if _, err := d.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{VolumeId: "pvc-gone", StagingTargetPath: staging}); err != nil {
		t.Fatalf("unstage of an unknown volume must be a no-op: %v", err)
	}
	// and the real runner treats a missing mount point the same way
	r := NewDfuseRunner(newFakeMounts())
	if err := r.Stop(ctx, filepath.Join(t.TempDir(), "not-there")); err != nil {
		t.Fatalf("Stop on a missing path: %v", err)
	}
}

func TestNodeGetVolumeStats(t *testing.T) {
	ctx := context.Background()
	fuse, mounts := newFakeFuse(), newFakeMounts()
	d := newTestDriver(t, newFakeCRs(), fuse, mounts)
	staging, target := filepath.Join(t.TempDir(), "staging"), t.TempDir()
	vc := map[string]string{ctxPool: "kv", ctxContainer: "pvc-1"}
	if _, err := d.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{VolumeId: "pvc-1", StagingTargetPath: staging, VolumeCapability: rwx(), VolumeContext: vc}); err != nil {
		t.Fatal(err)
	}
	resp, err := d.NodeGetVolumeStats(ctx, &csi.NodeGetVolumeStatsRequest{VolumeId: "pvc-1", VolumePath: target})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Usage) == 0 || resp.Usage[0].Unit != csi.VolumeUsage_BYTES || resp.Usage[0].Total <= 0 {
		t.Fatalf("usage: %+v", resp.Usage)
	}
	if resp.Usage[0].Used+resp.Usage[0].Available > resp.Usage[0].Total+resp.Usage[0].Total/100 {
		t.Errorf("used+available must be within total: %+v", resp.Usage[0])
	}
	// the driver advertises the capability it implements
	caps, _ := d.NodeGetCapabilities(ctx, &csi.NodeGetCapabilitiesRequest{})
	var hasStats bool
	for _, c := range caps.Capabilities {
		if c.GetRpc().GetType() == csi.NodeServiceCapability_RPC_GET_VOLUME_STATS {
			hasStats = true
		}
	}
	if !hasStats {
		t.Error("GET_VOLUME_STATS must be advertised")
	}
	for _, bad := range []*csi.NodeGetVolumeStatsRequest{
		{VolumePath: target}, {VolumeId: "pvc-1"},
	} {
		if _, err := d.NodeGetVolumeStats(ctx, bad); status.Code(err) != codes.InvalidArgument {
			t.Errorf("bad request %+v: %v", bad, err)
		}
	}
	if _, err := d.NodeGetVolumeStats(ctx, &csi.NodeGetVolumeStatsRequest{VolumeId: "pvc-1", VolumePath: filepath.Join(target, "nope")}); status.Code(err) != codes.NotFound {
		t.Errorf("missing path must be NotFound: %v", err)
	}
}

func TestGetCapacityFollowsThePool(t *testing.T) {
	ctx := context.Background()
	crs := newFakeCRs()
	d := newTestDriver(t, crs, newFakeFuse(), newFakeMounts())
	resp, err := d.GetCapacity(ctx, &csi.GetCapacityRequest{Parameters: map[string]string{ParamPool: "kv"}})
	if err != nil || resp.AvailableCapacity != 100<<30 || resp.MaximumVolumeSize.GetValue() != 100<<30 {
		t.Fatalf("%+v %v", resp, err)
	}
	// unknown pool or no parameter: zero, not a guess
	for _, p := range []map[string]string{{ParamPool: "nope"}, {}} {
		resp, err = d.GetCapacity(ctx, &csi.GetCapacityRequest{Parameters: p})
		if err != nil || resp.AvailableCapacity != 0 {
			t.Fatalf("%v: %+v %v", p, resp, err)
		}
	}
	caps, _ := d.ControllerGetCapabilities(ctx, &csi.ControllerGetCapabilitiesRequest{})
	var hasCap bool
	for _, c := range caps.Capabilities {
		if c.GetRpc().GetType() == csi.ControllerServiceCapability_RPC_GET_CAPACITY {
			hasCap = true
		}
	}
	if !hasCap {
		t.Error("GET_CAPACITY must be advertised")
	}
}

// TestSanity runs kubernetes-csi/csi-test against the whole driver with fakes.
func TestSanity(t *testing.T) {
	dir := t.TempDir()
	sock := filepath.Join(dir, "csi.sock")
	crs, fuse, mounts := newFakeCRs(), newFakeFuse(), newFakeMounts()
	d, err := New(Config{Endpoint: "unix://" + sock, NodeID: "node-a", Containers: crs, Namespace: "daos-csi", Mounter: mounts, Fuse: fuse, StagingDir: filepath.Join(dir, "state")})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = d.Run(ctx) }()
	cfg := sanity.NewTestConfig()
	cfg.Address = "unix://" + sock
	cfg.TargetPath = filepath.Join(dir, "target")
	cfg.StagingPath = filepath.Join(dir, "staging")
	cfg.TestVolumeParameters = map[string]string{ParamPool: "kv"}
	cfg.TestVolumeSize = 1 << 30
	cfg.IdempotentCount = 2
	sanity.Test(t, cfg)
}
