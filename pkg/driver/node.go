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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

// Node plugin (ADR-csi-001): NodeStageVolume runs one dfuse per volume on a
// staging path under StagingDir (not kubelet's staging path, so it survives
// kubelet's cleanup and can be recovered), NodePublishVolume bind-mounts the
// staging path into the pod's target path. Volume state is written to
// StagingDir/state/<volume>.json so a restarted plugin re-creates the dfuse
// mounts (FUSE mounts die with their daemon).

type volumeState struct {
	VolumeID  string `json:"volumeId"`
	Pool      string `json:"pool"`
	Container string `json:"container"`
	Mount     string `json:"mount"`
}

func (d *Driver) stagePath(id string) string { return filepath.Join(d.cfg.StagingDir, "mounts", id) }
func (d *Driver) statePath(id string) string {
	return filepath.Join(d.cfg.StagingDir, "state", id+".json")
}

func (d *Driver) saveState(s volumeState) error {
	if err := os.MkdirAll(filepath.Dir(d.statePath(s.VolumeID)), 0o755); err != nil {
		return err
	}
	b, _ := json.Marshal(s)
	return os.WriteFile(d.statePath(s.VolumeID), b, 0o600)
}

func (d *Driver) loadState(id string) (*volumeState, error) {
	b, err := os.ReadFile(d.statePath(id))
	if err != nil {
		return nil, err
	}
	var s volumeState
	return &s, json.Unmarshal(b, &s)
}

// recoverMounts re-creates dfuse mounts recorded before a restart.
func (d *Driver) recoverMounts(ctx context.Context) error {
	entries, err := os.ReadDir(filepath.Join(d.cfg.StagingDir, "state"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var errs []string
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		s, err := d.loadState(strings.TrimSuffix(e.Name(), ".json"))
		if err != nil {
			errs = append(errs, e.Name()+": "+err.Error())
			continue
		}
		if err := d.cfg.Fuse.Start(ctx, s.Pool, s.Container, s.Mount); err != nil {
			errs = append(errs, s.VolumeID+": "+err.Error())
			continue
		}
		klog.InfoS("recovered dfuse mount", "volume", s.VolumeID, "pool", s.Pool, "container", s.Container)
	}
	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return nil
}

func (d *Driver) NodeGetInfo(context.Context, *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{NodeId: d.cfg.NodeID}, nil
}

func (d *Driver) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	// VOLUME_CONDITION is alpha and not in the released CSI spec package, so a
	// broken mount is reported as an error from NodeGetVolumeStats instead.
	rpc := []csi.NodeServiceCapability_RPC_Type{
		csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
		csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
	}
	caps := make([]*csi.NodeServiceCapability, 0, len(rpc))
	for _, t := range rpc {
		caps = append(caps, &csi.NodeServiceCapability{Type: &csi.NodeServiceCapability_Rpc{Rpc: &csi.NodeServiceCapability_RPC{Type: t}}})
	}
	return &csi.NodeGetCapabilitiesResponse{Capabilities: caps}, nil
}

// NodeGetVolumeStats answers what kubelet shows as PVC usage. DAOS has no
// per-container quota, so statfs on the dfuse mount reports the pool's numbers:
// that is what the application actually has left, which is the useful answer.
// A dead dfuse answers ENOTCONN here, and saying so is the point.
func (d *Driver) NodeGetVolumeStats(_ context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id is required")
	}
	path := req.GetVolumePath()
	if path == "" {
		return nil, status.Error(codes.InvalidArgument, "volume path is required")
	}
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return nil, status.Errorf(codes.NotFound, "volume path %s does not exist on node %s", path, d.cfg.NodeID)
		}
		return nil, status.Errorf(codes.Internal, "cannot stat %s: %v (is dfuse still running?)", path, err)
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return nil, status.Errorf(codes.Internal, "statfs %s: %v (is dfuse still running?)", path, err)
	}
	bs := int64(st.Bsize)
	total, free, avail := int64(st.Blocks)*bs, int64(st.Bfree)*bs, int64(st.Bavail)*bs
	usage := []*csi.VolumeUsage{{Unit: csi.VolumeUsage_BYTES, Total: total, Available: avail, Used: total - free}}
	if st.Files > 0 {
		usage = append(usage, &csi.VolumeUsage{Unit: csi.VolumeUsage_INODES, Total: int64(st.Files),
			Available: int64(st.Ffree), Used: int64(st.Files) - int64(st.Ffree)})
	}
	return &csi.NodeGetVolumeStatsResponse{Usage: usage}, nil
}

// NodeStageVolume starts dfuse on the driver's own staging path for the volume.
func (d *Driver) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	id := req.GetVolumeId()
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id is required")
	}
	if req.GetStagingTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path is required")
	}
	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "volume capability is required")
	}
	if req.GetVolumeCapability().GetBlock() != nil {
		return nil, status.Error(codes.InvalidArgument, "block volumes are not supported")
	}
	vc := req.GetVolumeContext()
	pool, cont := vc[ctxPool], vc[ctxContainer]
	if vc[ctxPoolUUID] != "" && vc[ctxContUUID] != "" {
		pool, cont = vc[ctxPoolUUID], vc[ctxContUUID] // UUIDs survive label changes
	}
	if pool == "" || cont == "" {
		return nil, status.Errorf(codes.InvalidArgument, "volume context needs %s and %s", ctxPool, ctxContainer)
	}
	mnt := d.stagePath(id)
	// Start is idempotent and is the only thing that knows whether the mount is
	// really there; a Running() check here would publish a directory that dfuse
	// failed to mount.
	if err := d.cfg.Fuse.Start(ctx, pool, cont, mnt); err != nil {
		return nil, status.Errorf(codes.Internal, "dfuse %s/%s at %s: %v", pool, cont, mnt, err)
	}
	if err := d.saveState(volumeState{VolumeID: id, Pool: pool, Container: cont, Mount: mnt}); err != nil {
		return nil, status.Errorf(codes.Internal, "record state: %v", err)
	}
	// expose the mount at kubelet's staging path too, so it is visible in the
	// usual place; publish binds from our path
	if err := d.bind(mnt, req.GetStagingTargetPath(), false); err != nil {
		return nil, status.Errorf(codes.Internal, "bind %s -> %s: %v", mnt, req.GetStagingTargetPath(), err)
	}
	klog.InfoS("staged", "volume", id, "pool", pool, "container", cont, "mount", mnt)
	return &csi.NodeStageVolumeResponse{}, nil
}

func (d *Driver) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	id := req.GetVolumeId()
	if id == "" || req.GetStagingTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id and staging target path are required")
	}
	if err := d.unbind(req.GetStagingTargetPath()); err != nil {
		return nil, status.Errorf(codes.Internal, "unmount %s: %v", req.GetStagingTargetPath(), err)
	}
	mnt := d.stagePath(id)
	if err := d.cfg.Fuse.Stop(ctx, mnt); err != nil {
		return nil, status.Errorf(codes.Internal, "stop dfuse %s: %v", mnt, err)
	}
	_ = os.Remove(d.statePath(id))
	_ = os.Remove(mnt)
	klog.InfoS("unstaged", "volume", id)
	return &csi.NodeUnstageVolumeResponse{}, nil
}

// NodePublishVolume bind-mounts the staged dfuse mount into the pod.
func (d *Driver) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	id := req.GetVolumeId()
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id is required")
	}
	if req.GetTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "target path is required")
	}
	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "volume capability is required")
	}
	if req.GetStagingTargetPath() == "" {
		return nil, status.Error(codes.FailedPrecondition, "volume must be staged first")
	}
	mnt := d.stagePath(id)
	if !d.cfg.Fuse.Running(mnt) {
		// plugin restarted and recovery did not bring it back: try once more from state
		s, err := d.loadState(id)
		if err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "volume %s is not staged on this node", id)
		}
		if err := d.cfg.Fuse.Start(ctx, s.Pool, s.Container, mnt); err != nil {
			return nil, status.Errorf(codes.Internal, "dfuse: %v", err)
		}
	}
	if err := d.bind(mnt, req.GetTargetPath(), req.GetReadonly()); err != nil {
		return nil, status.Errorf(codes.Internal, "bind %s -> %s: %v", mnt, req.GetTargetPath(), err)
	}
	klog.InfoS("published", "volume", id, "target", req.GetTargetPath(), "readonly", req.GetReadonly())
	return &csi.NodePublishVolumeResponse{}, nil
}

func (d *Driver) NodeUnpublishVolume(_ context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if req.GetVolumeId() == "" || req.GetTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id and target path are required")
	}
	if err := d.unbind(req.GetTargetPath()); err != nil {
		return nil, status.Errorf(codes.Internal, "unmount %s: %v", req.GetTargetPath(), err)
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// bind mounts src at dst (idempotent).
// notMountedErr reports whether umount(8) failed only because nothing was
// mounted there.
func notMountedErr(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "not mounted") || strings.Contains(msg, "not currently mounted") ||
		strings.Contains(msg, "no mount point specified")
}

func (d *Driver) bind(src, dst string, readonly bool) error {
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}
	notMnt, err := d.cfg.Mounter.IsLikelyNotMountPoint(dst)
	if err != nil {
		return err
	}
	if !notMnt {
		return nil
	}
	opts := []string{"bind"}
	if readonly {
		opts = append(opts, "ro")
	}
	return d.cfg.Mounter.Mount(src, dst, "", opts)
}

// unbind unmounts dst if mounted and removes the directory (idempotent).
func (d *Driver) unbind(dst string) error {
	if _, err := d.stat(dst); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		// ENOTCONN is a dfuse that died under the mount (the node plugin was
		// restarted, say). The mount is still there and is exactly what has to go,
		// so this is a reason to unmount, not to give up.
		if !errors.Is(err, syscall.ENOTCONN) {
			return err
		}
	}
	// IsLikelyNotMountPoint compares st_dev with the parent, so it cannot see a
	// bind mount that stays on the same filesystem -- which is what dst is when
	// the staged path is not a dfuse mount. Unmount unconditionally and accept
	// "not mounted".
	if err := d.cfg.Mounter.Unmount(dst); err != nil && !notMountedErr(err) {
		return err
	}
	if err := os.Remove(dst); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
