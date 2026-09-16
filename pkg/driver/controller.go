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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"k8s.io/klog/v2"
)

// StorageClass parameters (all optional except pool). Defaults follow the
// lmcache-daos measurements the operator's CRD also uses.
const (
	ParamPool          = "pool"            // DaosPool name (required)
	ParamOclass        = "oclass"          // file object class, default RP_2GX
	ParamDirOclass     = "dirOclass"       // default RP_2G1
	ParamChunkSize     = "chunkSize"       // bytes, default 4194304
	ParamRdFac         = "rdFac"           // redundancy factor
	ParamCsum          = "csum"            // checksum, default crc32
	ParamNamespace     = "namespace"       // where DaosContainer CRs go (default: driver --namespace)
	ParamDestroyOnDele = "destroyOnDelete" // "true" (default): PV delete destroys the DAOS container

	// volume context keys handed to the node plugin
	ctxPool          = "pool"
	ctxContainer     = "container"
	ctxPoolUUID      = "poolUUID"
	ctxContUUID      = "containerUUID"
	ctxNamespace     = "namespace"
	ctxCRName        = "crName"
	createWaitBudget = 20 * time.Second
	createPoll       = 2 * time.Second
)

var labelRe = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,127}$`)

func (d *Driver) ControllerGetCapabilities(context.Context, *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	caps := []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_GET_CAPACITY,
	}
	out := make([]*csi.ControllerServiceCapability, 0, len(caps))
	for _, c := range caps {
		out = append(out, &csi.ControllerServiceCapability{Type: &csi.ControllerServiceCapability_Rpc{Rpc: &csi.ControllerServiceCapability_RPC{Type: c}}})
	}
	return &csi.ControllerGetCapabilitiesResponse{Capabilities: out}, nil
}

// CreateVolume creates (or finds) the DaosContainer named after the PV and
// waits a bounded time for the operator to report it Ready. Volume ID = CR
// name (pvc-<uid>): stable from the first call, so retries are idempotent;
// pool/container labels and UUIDs travel in the volume context.
func (d *Driver) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	name := req.GetName()
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	if len(req.GetVolumeCapabilities()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume capabilities are required")
	}
	for _, c := range req.GetVolumeCapabilities() {
		if c.GetBlock() != nil {
			return nil, status.Error(codes.InvalidArgument, "block volumes are not supported; DAOS containers are mounted with dfuse (filesystem)")
		}
		switch c.GetAccessMode().GetMode() {
		case csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY,
			csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY, csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER, csi.VolumeCapability_AccessMode_SINGLE_NODE_MULTI_WRITER:
		default:
			return nil, status.Errorf(codes.InvalidArgument, "unsupported access mode %s", c.GetAccessMode().GetMode())
		}
	}
	p := req.GetParameters()
	poolName := p[ParamPool]
	if poolName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "StorageClass parameter %q is required", ParamPool)
	}
	ns := p[ParamNamespace]
	if ns == "" {
		ns = d.cfg.Namespace
	}
	spec := ContainerSpec{Namespace: ns, Name: name, PoolRef: poolName, Label: containerLabel(name), Type: "POSIX", VolumeID: name,
		FileOclass: p[ParamOclass], DirOclass: p[ParamDirOclass], Checksum: p[ParamCsum],
		CapacityBytes: req.GetCapacityRange().GetRequiredBytes()}
	if !labelRe.MatchString(spec.Label) {
		return nil, status.Errorf(codes.InvalidArgument, "%q is not a valid DAOS container label", spec.Label)
	}
	if v := p[ParamChunkSize]; v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n <= 0 {
			return nil, status.Errorf(codes.InvalidArgument, "%s must be a positive integer (bytes): %q", ParamChunkSize, v)
		}
		spec.ChunkSize = n
	}
	if v := p[ParamRdFac]; v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 || n > 4 {
			return nil, status.Errorf(codes.InvalidArgument, "%s must be 0..4: %q", ParamRdFac, v)
		}
		rf := int32(n)
		spec.RedundancyFac = &rf
	}
	for k, v := range p {
		if strings.HasPrefix(k, "property.") {
			if spec.Properties == nil {
				spec.Properties = map[string]string{}
			}
			spec.Properties[strings.TrimPrefix(k, "property.")] = v
		}
	}

	pool, err := d.cfg.Containers.GetPool(ctx, poolName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get DaosPool %s: %v", poolName, err)
	}
	if !pool.Exists {
		return nil, status.Errorf(codes.NotFound, "DaosPool %s does not exist", poolName)
	}
	if pool.UUID == "" {
		return nil, status.Errorf(codes.Unavailable, "DaosPool %s has no DAOS pool yet (operator still creating it)", poolName)
	}
	if want := req.GetCapacityRange().GetRequiredBytes(); want > 0 && pool.FreeBytes > 0 && want > pool.FreeBytes {
		return nil, status.Errorf(codes.OutOfRange, "requested %d bytes but pool %s reports %d free (DAOS 2.8 enforces capacity per pool, not per container)", want, poolName, pool.FreeBytes)
	}

	// idempotency: same name with a different size is a different volume
	if existing, err := d.cfg.Containers.GetContainer(ctx, ns, name); err == nil && existing.Exists &&
		existing.CapacityBytes != 0 && spec.CapacityBytes != 0 && existing.CapacityBytes != spec.CapacityBytes {
		return nil, status.Errorf(codes.AlreadyExists, "volume %s exists with %d bytes, requested %d", name, existing.CapacityBytes, spec.CapacityBytes)
	}
	if err := d.cfg.Containers.CreateContainer(ctx, spec); err != nil {
		return nil, status.Errorf(codes.Internal, "create DaosContainer %s/%s: %v", ns, name, err)
	}
	// wait for the operator, but not longer than the provisioner is willing to
	deadline := time.Now().Add(createWaitBudget)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl.Add(-2 * time.Second)
	}
	var st *ContainerStatus
	for {
		st, err = d.cfg.Containers.GetContainer(ctx, ns, name)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "get DaosContainer %s/%s: %v", ns, name, err)
		}
		if st.Ready && st.UUID != "" {
			break
		}
		if strings.HasSuffix(st.Reason, "Failed") || st.Reason == "ContainerMissing" || st.Reason == "PoolNotFound" {
			return nil, status.Errorf(codes.Internal, "DaosContainer %s/%s: %s: %s", ns, name, st.Reason, st.Message)
		}
		if time.Now().After(deadline) {
			// provisioner retries with exponential backoff; the CR keeps progressing meanwhile
			return nil, status.Errorf(codes.Unavailable, "DaosContainer %s/%s not ready yet (%s: %s); retry", ns, name, st.Reason, st.Message)
		}
		select {
		case <-ctx.Done():
			return nil, status.Error(codes.Canceled, ctx.Err().Error())
		case <-time.After(createPoll):
		}
	}
	klog.InfoS("volume ready", "volume", name, "pool", pool.Label, "container", st.UUID)
	return &csi.CreateVolumeResponse{Volume: &csi.Volume{
		VolumeId:      name,
		CapacityBytes: req.GetCapacityRange().GetRequiredBytes(),
		VolumeContext: map[string]string{
			ctxPool: pool.Label, ctxContainer: spec.Label, ctxPoolUUID: pool.UUID, ctxContUUID: st.UUID,
			ctxNamespace: ns, ctxCRName: name, ParamDestroyOnDele: destroyOnDelete(p),
		},
	}}, nil
}

// containerLabel keeps DAOS's 127-byte label limit: long PV names are shortened
// with a hash suffix (the CR keeps the full name).
func containerLabel(name string) string {
	if len(name) <= 127 {
		return name
	}
	h := sha256.Sum256([]byte(name))
	return name[:118] + "-" + hex.EncodeToString(h[:4])
}

func destroyOnDelete(p map[string]string) string {
	if v, ok := p[ParamDestroyOnDele]; ok && strings.EqualFold(v, "false") {
		return "false"
	}
	return "true"
}

// DeleteVolume deletes the DaosContainer CR. With destroyOnDelete (default) the
// destroy-approved annotation is set so the operator really removes the DAOS
// container; otherwise the operator orphans it (reclaim by hand).
func (d *Driver) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	id := req.GetVolumeId()
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id is required")
	}
	// the external-provisioner does not pass the volume context here; the CR
	// namespace comes from the driver default unless encoded as ns/name
	ns, name := d.cfg.Namespace, id
	if i := strings.IndexByte(id, '/'); i > 0 {
		ns, name = id[:i], id[i+1:]
	}
	destroy := true
	if v, ok := req.GetSecrets()[ParamDestroyOnDele]; ok && strings.EqualFold(v, "false") {
		destroy = false
	}
	if err := d.cfg.Containers.DeleteContainer(ctx, ns, name, destroy); err != nil {
		return nil, status.Errorf(codes.Internal, "delete DaosContainer %s/%s: %v", ns, name, err)
	}
	klog.InfoS("volume deleted", "volume", id, "destroy", destroy)
	return &csi.DeleteVolumeResponse{}, nil
}

func (d *Driver) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id is required")
	}
	if len(req.GetVolumeCapabilities()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume capabilities are required")
	}
	ns, name := d.cfg.Namespace, req.GetVolumeId()
	if v := req.GetVolumeContext()[ctxNamespace]; v != "" {
		ns = v
	}
	st, err := d.cfg.Containers.GetContainer(ctx, ns, name)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	if !st.Exists {
		return nil, status.Errorf(codes.NotFound, "volume %s not found", req.GetVolumeId())
	}
	for _, c := range req.GetVolumeCapabilities() {
		if c.GetBlock() != nil {
			return &csi.ValidateVolumeCapabilitiesResponse{Message: "block volumes are not supported"}, nil
		}
	}
	return &csi.ValidateVolumeCapabilitiesResponse{Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
		VolumeContext: req.GetVolumeContext(), VolumeCapabilities: req.GetVolumeCapabilities(), Parameters: req.GetParameters()}}, nil
}

// GetCapacity reports what the pool behind a StorageClass has left. DAOS
// enforces capacity per pool, not per container, so every volume from the same
// StorageClass shares this number; the external-provisioner turns it into
// CSIStorageCapacity objects when it runs with --enable-capacity.
func (d *Driver) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	name := req.GetParameters()[ParamPool]
	if name == "" {
		// no pool named: nothing useful to say, and guessing would be worse
		return &csi.GetCapacityResponse{AvailableCapacity: 0}, nil
	}
	pool, err := d.cfg.Containers.GetPool(ctx, name)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get DaosPool %s: %v", name, err)
	}
	if !pool.Exists || pool.UUID == "" {
		return &csi.GetCapacityResponse{AvailableCapacity: 0}, nil
	}
	return &csi.GetCapacityResponse{
		AvailableCapacity: pool.FreeBytes,
		MaximumVolumeSize: wrapperspb.Int64(pool.FreeBytes),
	}, nil
}

func (d *Driver) ControllerPublishVolume(context.Context, *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "DAOS volumes need no attach step")
}

func (d *Driver) ControllerUnpublishVolume(context.Context, *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "DAOS volumes need no attach step")
}

// String helper for tests/logs.
func volumeSummary(v *csi.Volume) string {
	return fmt.Sprintf("%s pool=%s cont=%s", v.GetVolumeId(), v.GetVolumeContext()[ctxPool], v.GetVolumeContext()[ctxContainer])
}
