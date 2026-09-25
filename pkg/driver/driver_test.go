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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

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

	// the state file must remember kubelet's staging path, or recovery cannot repair it
	if st, err := d.loadState("pvc-1"); err != nil || st.StagingTarget != staging {
		t.Fatalf("state must record the staging target: %+v %v", st, err)
	}

	// plugin restart: dfuse is gone, and kubelet's staging bind of it is now a
	// dead FUSE mount (every stat says ENOTCONN). A new Driver with the same
	// StagingDir must clear that bind, bring dfuse back and bind it again --
	// otherwise kubelet fails MountDevice on the dead directory and never calls
	// NodeStage (exaci4-2, 2026-09-26).
	dead := &deadMounts{fakeMounts: mounts, dead: map[string]bool{staging: true}}
	fuse2 := newFakeFuse()
	d2, _ := New(Config{Endpoint: "unix:///tmp/x.sock", NodeID: "node-a", Containers: newFakeCRs(), Namespace: "n", Mounter: dead, Fuse: fuse2, StagingDir: d.cfg.StagingDir})
	d2.stat = dead.stat
	if err := d2.recoverMounts(ctx); err != nil {
		t.Fatal(err)
	}
	if !fuse2.Running(mnt) {
		t.Fatal("state file must bring dfuse back after a restart")
	}
	if dead.dead[staging] {
		t.Fatal("recovery must clear the dead staging bind")
	}
	if mounts.mounts[staging] != mnt {
		t.Fatalf("recovery must bind the staging path to the new dfuse mount again: %v", mounts.mounts)
	}
	if !dead.unmounted[staging] {
		t.Fatal("the dead bind must be unmounted, not just forgotten")
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

// A bind mount that stays on the same filesystem is invisible to
// IsLikelyNotMountPoint (it compares st_dev with the parent), so unbind used to
// skip the unmount and then fail to remove the directory with EBUSY, wedging
// NodeUnpublish forever (exaci4-2, 2026-09-22).
func TestUnbindUnmountsABindTheDeviceCheckCannotSee(t *testing.T) {
	mounts := newFakeMounts()
	d, err := New(Config{Endpoint: "unix://" + filepath.Join(t.TempDir(), "csi.sock"), NodeID: "node-a",
		Containers: newFakeCRs(), Namespace: "daos-csi", Mounter: sameDeviceMounts{mounts},
		Fuse: newFakeFuse(), StagingDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(t.TempDir(), "target")
	if err := d.bind(filepath.Join(t.TempDir(), "src"), dst, false); err != nil {
		t.Fatal(err)
	}
	if err := d.unbind(dst); err != nil {
		t.Fatalf("unbind: %v", err)
	}
	if len(mounts.mounts) != 0 {
		t.Fatalf("bind must be unmounted: %v", mounts.mounts)
	}
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Fatalf("unbind must remove the directory: %v", err)
	}
	// and it stays idempotent once everything is gone
	if err := d.unbind(dst); err != nil {
		t.Fatalf("second unbind: %v", err)
	}
}

// sameDeviceMounts mounts and unmounts for real (in the map) but always claims
// the path is not a mountpoint, like a bind within one filesystem.
type sameDeviceMounts struct{ *fakeMounts }

func (sameDeviceMounts) IsLikelyNotMountPoint(string) (bool, error) { return true, nil }

// A dfuse that died under the staging bind makes every stat there fail with
// ENOTCONN. unbind used to give up on that error, so NodeUnstageVolume retried
// forever and kubelet could never re-create the directory (exaci4-2,
// 2026-09-22, after a node plugin restart).
func TestUnbindClearsAMountWhoseFuseIsDead(t *testing.T) {
	mounts := newFakeMounts()
	d, err := New(Config{Endpoint: "unix://" + filepath.Join(t.TempDir(), "csi.sock"), NodeID: "node-a",
		Containers: newFakeCRs(), Namespace: "daos-csi", Mounter: mounts,
		Fuse: newFakeFuse(), StagingDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(t.TempDir(), "globalmount")
	if err := os.MkdirAll(dst, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := mounts.Mount("src", dst, "", []string{"bind"}); err != nil {
		t.Fatal(err)
	}
	d.stat = func(string) (os.FileInfo, error) {
		return nil, &os.PathError{Op: "stat", Path: dst, Err: syscall.ENOTCONN}
	}
	if err := d.unbind(dst); err != nil {
		t.Fatalf("unbind on a dead fuse mount: %v", err)
	}
	if len(mounts.mounts) != 0 {
		t.Fatalf("the dead mount must be unmounted: %v", mounts.mounts)
	}
}

// deadMounts is a fakeMounts whose listed paths behave like a FUSE mount whose
// daemon died: stat and IsLikelyNotMountPoint answer ENOTCONN until Unmount.
type deadMounts struct {
	*fakeMounts
	dead      map[string]bool
	unmounted map[string]bool
}

func (m *deadMounts) IsLikelyNotMountPoint(p string) (bool, error) {
	if m.dead[p] {
		return false, &os.PathError{Op: "stat", Path: p, Err: syscall.ENOTCONN}
	}
	return m.fakeMounts.IsLikelyNotMountPoint(p)
}

func (m *deadMounts) Unmount(p string) error {
	if m.dead[p] {
		delete(m.dead, p)
		if m.unmounted == nil {
			m.unmounted = map[string]bool{}
		}
		m.unmounted[p] = true
		delete(m.fakeMounts.mounts, p)
		return nil
	}
	return m.fakeMounts.Unmount(p)
}

func (m *deadMounts) stat(p string) (os.FileInfo, error) {
	if m.dead[p] {
		return nil, &os.PathError{Op: "stat", Path: p, Err: syscall.ENOTCONN}
	}
	return os.Stat(p)
}

// bind must be able to replace a dead FUSE mount at its destination, since
// that is exactly what kubelet's staging path is after a plugin restart.
func TestBindReplacesADeadMountAtTheDestination(t *testing.T) {
	base := newFakeMounts()
	dst := filepath.Join(t.TempDir(), "globalmount")
	if err := os.MkdirAll(dst, 0o755); err != nil {
		t.Fatal(err)
	}
	base.mounts[dst] = "old-dfuse"
	dead := &deadMounts{fakeMounts: base, dead: map[string]bool{dst: true}}
	d, err := New(Config{Endpoint: "unix://" + filepath.Join(t.TempDir(), "csi.sock"), NodeID: "node-a",
		Containers: newFakeCRs(), Namespace: "daos-csi", Mounter: dead, Fuse: newFakeFuse(), StagingDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	d.stat = dead.stat
	if err := d.bind("new-dfuse", dst, false); err != nil {
		t.Fatalf("bind over a dead mount: %v", err)
	}
	if base.mounts[dst] != "new-dfuse" {
		t.Fatalf("destination must now be bound to the new mount: %v", base.mounts)
	}
}

// mountinfo parsing: only fuse mounts below the given prefix, path escapes decoded.
func TestFuseMountsUnder(t *testing.T) {
	mi := `1609 96 0:112 / /var/lib/kubelet/plugins/kubernetes.io/csi/pod.daos.csi.gluesys.com/b01d/globalmount rw,nosuid - fuse.daos dfuse rw
1610 96 0:113 / /var/lib/kubelet/plugins/kubernetes.io/csi/other.csi.io/aaaa/globalmount rw - fuse.daos dfuse rw
1611 96 253:0 /x /var/lib/kubelet/plugins/kubernetes.io/csi/pod.daos.csi.gluesys.com/cccc/globalmount rw - xfs /dev/mapper/rl-root rw
1612 96 0:114 / /var/lib/kubelet/plugins/kubernetes.io/csi/pod.daos.csi.gluesys.com/with\040space/globalmount rw - fuse.daos dfuse rw
garbage line
`
	got := fuseMountsUnder(strings.NewReader(mi), "/var/lib/kubelet/plugins/kubernetes.io/csi/pod.daos.csi.gluesys.com/")
	want := []string{
		"/var/lib/kubelet/plugins/kubernetes.io/csi/pod.daos.csi.gluesys.com/b01d/globalmount",
		"/var/lib/kubelet/plugins/kubernetes.io/csi/pod.daos.csi.gluesys.com/with space/globalmount",
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// A state file from before StagingTarget existed cannot say where kubelet's
// bind is, so recovery must find dead binds from kubelet's own layout.
func TestRecoverySweepsDeadGlobalMountsWithoutStateHelp(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	kubelet := filepath.Join(dir, "kubelet")
	deadGM := filepath.Join(kubelet, "plugins/kubernetes.io/csi/test.csi/hash1/globalmount")
	liveGM := filepath.Join(kubelet, "plugins/kubernetes.io/csi/test.csi/hash2/globalmount")
	foreign := filepath.Join(kubelet, "plugins/kubernetes.io/csi/other.csi/hash3/globalmount")
	mi := filepath.Join(dir, "mountinfo")
	body := ""
	for i, mp := range []string{deadGM, liveGM, foreign} {
		body += fmt.Sprintf("%d 96 0:%d / %s rw - fuse.daos dfuse rw\n", 1600+i, 200+i, mp)
	}
	if err := os.WriteFile(mi, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	base := newFakeMounts()
	for _, mp := range []string{deadGM, liveGM, foreign} {
		base.mounts[mp] = "dfuse"
	}
	dead := &deadMounts{fakeMounts: base, dead: map[string]bool{deadGM: true, foreign: true}}
	d, err := New(Config{Name: "test.csi", Endpoint: "unix:///tmp/x.sock", NodeID: "n", Containers: newFakeCRs(), Namespace: "ns",
		Mounter: dead, Fuse: newFakeFuse(), StagingDir: filepath.Join(dir, "staging"), KubeletDir: kubelet})
	if err != nil {
		t.Fatal(err)
	}
	d.stat, d.mountinfo = dead.stat, mi
	if err := d.recoverMounts(ctx); err != nil {
		t.Fatal(err)
	}
	if !dead.unmounted[deadGM] {
		t.Fatal("the dead bind under this driver's directory must be cleared")
	}
	if _, ok := base.mounts[liveGM]; !ok {
		t.Fatal("a healthy bind must be left alone")
	}
	if dead.unmounted[foreign] {
		t.Fatal("another driver's bind is not ours to touch")
	}
}

// dfuse that does not come back at startup (agent not answering yet) is retried
// in the background until it does, and the staging path is rebound then.
func TestRecoveryRetriesUntilDfuseComesBack(t *testing.T) {
	ctx := context.Background()
	fuse, mounts := newFakeFuse(), newFakeMounts()
	d := newTestDriver(t, newFakeCRs(), fuse, mounts)
	staging := filepath.Join(t.TempDir(), "staging")
	vc := map[string]string{ctxPool: "kv", ctxContainer: "pvc-9", ctxPoolUUID: "pu", ctxContUUID: "cu"}
	if _, err := d.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{VolumeId: "pvc-9", StagingTargetPath: staging, VolumeCapability: rwx(), VolumeContext: vc}); err != nil {
		t.Fatal(err)
	}
	mnt := d.stagePath("pvc-9")
	delete(mounts.mounts, staging) // the restart took the bind with it

	fuse2 := newFakeFuse()
	fuse2.failTimes = 2
	d2, _ := New(Config{Endpoint: "unix:///tmp/x.sock", NodeID: "n", Containers: newFakeCRs(), Namespace: "ns", Mounter: mounts, Fuse: fuse2, StagingDir: d.cfg.StagingDir})
	d2.recoveryRetries, d2.recoveryInterval = 5, 20*time.Millisecond
	if err := d2.recoverMounts(ctx); err == nil {
		t.Fatal("the first attempt is expected to fail here")
	}
	deadline := time.Now().Add(3 * time.Second)
	for !fuse2.Running(mnt) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !fuse2.Running(mnt) {
		t.Fatal("background retries must bring dfuse back")
	}
	deadline = time.Now().Add(time.Second)
	for mounts.mounts[staging] != mnt && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if mounts.mounts[staging] != mnt {
		t.Fatalf("staging path must be rebound after the retry succeeds: %v", mounts.mounts)
	}
}
