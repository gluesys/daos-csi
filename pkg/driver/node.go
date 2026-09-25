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
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

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
	// StagingTarget is kubelet's staging path (the "globalmount"). It is a bind
	// of Mount, so when the plugin restarts and dfuse dies it turns into a dead
	// FUSE mount that kubelet itself trips over (MountDevice cannot mkdir it)
	// before it ever calls NodeStage. Recovery has to repair it.
	StagingTarget string `json:"stagingTarget,omitempty"`
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
//
// A restart kills every dfuse (they are children of the plugin), and each
// kubelet staging bind of those mounts becomes a dead FUSE mount. kubelet then
// fails MountDevice on "mkdir globalmount: transport endpoint is not
// connected" and never reaches NodeStage, so nothing on the CSI side can help
// (exaci4-2, 2026-09-26). Clearing those dead binds is therefore done first
// and unconditionally: even when dfuse cannot be brought back here, kubelet
// can then call NodeStage, which starts it.
func (d *Driver) recoverMounts(ctx context.Context) error {
	entries, err := os.ReadDir(filepath.Join(d.cfg.StagingDir, "state"))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	// no state directory means nothing was ever staged by this plugin -- but a
	// previous version may still have left binds behind, so the sweep runs anyway
	var states []*volumeState
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
		states = append(states, s)
		if s.StagingTarget != "" {
			if err := d.clearDeadMount(s.StagingTarget); err != nil {
				errs = append(errs, s.VolumeID+": staging bind: "+err.Error())
			}
		}
	}
	// State files written before StagingTarget existed do not say where the
	// bind is; kubelet's layout does. Sweep this driver's directory as well.
	if err := d.sweepDeadGlobalMounts(); err != nil {
		errs = append(errs, "sweep: "+err.Error())
	}
	if len(states) > 0 {
		// the agent is a native sidecar: started before us, not necessarily
		// listening yet. The dmg/daos Jobs wait for the socket the same way.
		waitForAgentSocket(ctx, 60*time.Second)
	}
	var pending []*volumeState
	for _, s := range states {
		if err := d.recoverOne(ctx, s); err != nil {
			errs = append(errs, s.VolumeID+": "+err.Error())
			pending = append(pending, s)
		}
	}
	if len(pending) > 0 {
		// Not fatal: the dead binds are cleared, so kubelet can call NodeStage
		// and start dfuse itself. Meanwhile keep trying, in case nothing asks.
		go d.retryRecovery(ctx, pending)
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
	if err := d.saveState(volumeState{VolumeID: id, Pool: pool, Container: cont, Mount: mnt,
		StagingTarget: req.GetStagingTargetPath()}); err != nil {
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

// recoverOne brings one recorded mount back and rebinds kubelet's staging path to it.
func (d *Driver) recoverOne(ctx context.Context, s *volumeState) error {
	if err := d.cfg.Fuse.Start(ctx, s.Pool, s.Container, s.Mount); err != nil {
		return err
	}
	if s.StagingTarget != "" {
		if err := d.bind(s.Mount, s.StagingTarget, false); err != nil {
			return fmt.Errorf("rebind staging: %w", err)
		}
	}
	klog.InfoS("recovered dfuse mount", "volume", s.VolumeID, "pool", s.Pool, "container", s.Container)
	return nil
}

// retryRecovery keeps trying the mounts that did not come back at startup.
func (d *Driver) retryRecovery(ctx context.Context, pending []*volumeState) {
	for attempt := 1; attempt <= d.recoveryRetries && len(pending) > 0; attempt++ {
		select {
		case <-ctx.Done():
			return
		case <-time.After(d.recoveryInterval):
		}
		var still []*volumeState
		for _, s := range pending {
			if err := d.recoverOne(ctx, s); err != nil {
				klog.InfoS("mount recovery retry failed", "volume", s.VolumeID, "attempt", attempt, "err", err)
				still = append(still, s)
			}
		}
		pending = still
	}
	for _, s := range pending {
		klog.ErrorS(nil, "mount recovery gave up; NodeStage will start dfuse when a pod asks", "volume", s.VolumeID)
	}
}

// sweepDeadGlobalMounts finds this driver's staging binds in kubelet's plugin
// directory (<kubeletDir>/plugins/kubernetes.io/csi/<driver>/<hash>/globalmount)
// and clears the ones whose dfuse is gone.
func (d *Driver) sweepDeadGlobalMounts() error {
	prefix := filepath.Join(d.cfg.KubeletDir, "plugins", "kubernetes.io", "csi", d.cfg.Name) + string(filepath.Separator)
	f, err := os.Open(d.mountinfo)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	var errs []string
	for _, mp := range fuseMountsUnder(f, prefix) {
		if err := d.clearDeadMount(mp); err != nil {
			errs = append(errs, mp+": "+err.Error())
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return nil
}

// fuseMountsUnder returns the mount points of fuse.daos mounts below prefix,
// read from a mountinfo(5) stream: "id parent maj:min root mountpoint opts ... - fstype source ...".
func fuseMountsUnder(r io.Reader, prefix string) []string {
	var out []string
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		sep := -1
		for i, f := range fields {
			if f == "-" {
				sep = i
				break
			}
		}
		if sep < 5 || sep+1 >= len(fields) {
			continue
		}
		mp := strings.ReplaceAll(fields[4], `\040`, " ")
		if strings.HasPrefix(fields[sep+1], "fuse") && strings.HasPrefix(mp, prefix) {
			out = append(out, mp)
		}
	}
	return out
}

// clearDeadMount unmounts path if it is a FUSE mount whose daemon is gone
// (every stat answers ENOTCONN). A missing or healthy path is left alone.
func (d *Driver) clearDeadMount(path string) error {
	_, err := d.stat(path)
	if err == nil || os.IsNotExist(err) {
		return nil
	}
	if !errors.Is(err, syscall.ENOTCONN) {
		return err
	}
	klog.InfoS("clearing dead FUSE mount", "path", path)
	if err := d.cfg.Mounter.Unmount(path); err != nil && !notMountedErr(err) {
		return err
	}
	return nil
}

// waitForAgentSocket blocks until the daos_agent socket exists or the wait
// runs out; without DAOS_AGENT_DRPC_DIR there is nothing to wait for.
func waitForAgentSocket(ctx context.Context, wait time.Duration) {
	dir := os.Getenv("DAOS_AGENT_DRPC_DIR")
	if dir == "" {
		return
	}
	sock := filepath.Join(dir, "daos_agent.sock")
	deadline := time.Now().Add(wait)
	for {
		if _, err := os.Stat(sock); err == nil {
			return
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			klog.InfoS("daos_agent socket not seen; recovering anyway", "socket", sock, "waited", wait)
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (d *Driver) bind(src, dst string, readonly bool) error {
	// a dead FUSE mount at dst makes MkdirAll fail with "file exists": clear it first
	if err := d.clearDeadMount(dst); err != nil {
		return err
	}
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
