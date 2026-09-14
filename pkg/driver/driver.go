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

// Package driver implements the DAOS CSI driver: the controller turns
// CreateVolume into a DaosContainer custom resource (the operator runs `daos
// cont create`), the node plugin mounts containers with dfuse. It never talks
// to DAOS directly on the control path; it only writes CRs and reads their
// status, so the operator stays the single place that runs dmg/daos.
package driver

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"k8s.io/klog/v2"
)

const (
	// DefaultDriverName is the CSI driver name (StorageClass.provisioner).
	DefaultDriverName = "daos.csi.gluesys.com"
	// Version is reported by GetPluginInfo.
	Version = "0.1.0"
)

// Mode selects which services a process serves.
type Mode string

const (
	ModeController Mode = "controller"
	ModeNode       Mode = "node"
	ModeAll        Mode = "all"
)

// Config wires a Driver.
type Config struct {
	Name     string
	NodeID   string
	Mode     Mode
	Endpoint string // unix:///csi/csi.sock
	// Controller side
	Containers ContainerClient // DaosContainer / DaosPool access
	Namespace  string          // where DaosContainer CRs are created
	// Node side
	Mounter    Mounter
	Fuse       FuseRunner
	StagingDir string // host dir where per-volume dfuse mounts and state live
}

// Driver is the gRPC server for the three CSI services.
type Driver struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedControllerServer
	csi.UnimplementedNodeServer

	cfg Config
	srv *grpc.Server
}

// New validates the config and returns a Driver.
func New(cfg Config) (*Driver, error) {
	if cfg.Name == "" {
		cfg.Name = DefaultDriverName
	}
	if cfg.Mode == "" {
		cfg.Mode = ModeAll
	}
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("endpoint is required")
	}
	if (cfg.Mode == ModeController || cfg.Mode == ModeAll) && cfg.Containers == nil {
		return nil, fmt.Errorf("controller mode needs a Kubernetes client")
	}
	if cfg.Mode == ModeNode || cfg.Mode == ModeAll {
		if cfg.NodeID == "" {
			return nil, fmt.Errorf("node mode needs --node-id")
		}
		if cfg.Mounter == nil || cfg.Fuse == nil {
			return nil, fmt.Errorf("node mode needs a mounter and a dfuse runner")
		}
		if cfg.StagingDir == "" {
			cfg.StagingDir = "/var/lib/daos-csi"
		}
	}
	return &Driver{cfg: cfg}, nil
}

// Run serves until ctx is cancelled.
func (d *Driver) Run(ctx context.Context) error {
	scheme, addr, err := parseEndpoint(d.cfg.Endpoint)
	if err != nil {
		return err
	}
	if scheme == "unix" {
		if err := os.MkdirAll(filepath.Dir(addr), 0o755); err != nil {
			return err
		}
		if err := os.Remove(addr); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	lis, err := net.Listen(scheme, addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", d.cfg.Endpoint, err)
	}
	d.srv = grpc.NewServer(grpc.UnaryInterceptor(logInterceptor))
	csi.RegisterIdentityServer(d.srv, d)
	if d.cfg.Mode == ModeController || d.cfg.Mode == ModeAll {
		csi.RegisterControllerServer(d.srv, d)
	}
	if d.cfg.Mode == ModeNode || d.cfg.Mode == ModeAll {
		if err := d.recoverMounts(ctx); err != nil {
			klog.ErrorS(err, "mount recovery")
		}
		csi.RegisterNodeServer(d.srv, d)
	}
	go func() {
		<-ctx.Done()
		d.srv.GracefulStop()
	}()
	klog.InfoS("daos-csi serving", "name", d.cfg.Name, "mode", d.cfg.Mode, "endpoint", d.cfg.Endpoint, "node", d.cfg.NodeID)
	return d.srv.Serve(lis)
}

func parseEndpoint(ep string) (string, string, error) {
	switch {
	case strings.HasPrefix(ep, "unix://"):
		return "unix", strings.TrimPrefix(ep, "unix://"), nil
	case strings.HasPrefix(ep, "tcp://"):
		return "tcp", strings.TrimPrefix(ep, "tcp://"), nil
	}
	return "", "", fmt.Errorf("endpoint must be unix:// or tcp://, got %q", ep)
}

func logInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	resp, err := handler(ctx, req)
	if err != nil {
		klog.V(2).InfoS("csi call failed", "method", info.FullMethod, "err", err)
	} else {
		klog.V(4).InfoS("csi call", "method", info.FullMethod)
	}
	return resp, err
}

// --- Identity

func (d *Driver) GetPluginInfo(context.Context, *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{Name: d.cfg.Name, VendorVersion: Version}, nil
}

func (d *Driver) GetPluginCapabilities(context.Context, *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{Capabilities: []*csi.PluginCapability{
		{Type: &csi.PluginCapability_Service_{Service: &csi.PluginCapability_Service{Type: csi.PluginCapability_Service_CONTROLLER_SERVICE}}},
	}}, nil
}

func (d *Driver) Probe(context.Context, *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{}, nil
}
