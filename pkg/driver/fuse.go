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
	"os/exec"
	"strings"
	"sync"
	"time"

	"k8s.io/klog/v2"
	"k8s.io/mount-utils"
)

// Mounter is the subset of k8s.io/mount-utils the node plugin uses.
type Mounter interface {
	Mount(source, target, fstype string, options []string) error
	Unmount(target string) error
	IsLikelyNotMountPoint(file string) (bool, error)
}

// FuseRunner starts and stops dfuse for one volume.
type FuseRunner interface {
	// Start runs dfuse for pool/container at mountpoint and returns once the
	// mount is visible (or fails).
	Start(ctx context.Context, pool, container, mountpoint string) error
	// Stop unmounts mountpoint and ends the dfuse process.
	Stop(ctx context.Context, mountpoint string) error
	// Running reports whether this process owns a dfuse for mountpoint.
	Running(mountpoint string) bool
}

// DfuseRunner runs the real dfuse binary in the foreground, one process per
// volume, as children of the node plugin. The mount lives as long as the
// process; recoverMounts re-creates them after a plugin restart.
type DfuseRunner struct {
	Bin     string   // dfuse
	Umount  string   // fusermount3
	Args    []string // extra dfuse flags (e.g. --disable-caching)
	Mounter Mounter

	mu    sync.Mutex
	procs map[string]*exec.Cmd
}

func NewDfuseRunner(m Mounter, extra ...string) *DfuseRunner {
	return &DfuseRunner{Bin: "dfuse", Umount: "fusermount3", Args: extra, Mounter: m, procs: map[string]*exec.Cmd{}}
}

func (r *DfuseRunner) Start(ctx context.Context, pool, container, mountpoint string) error {
	r.mu.Lock()
	if _, ok := r.procs[mountpoint]; ok {
		r.mu.Unlock()
		return nil
	}
	r.mu.Unlock()
	if err := os.MkdirAll(mountpoint, 0o755); err != nil {
		return err
	}
	// a stale FUSE mount from a previous plugin instance answers ENOTCONN: unmount it first
	if notMnt, err := r.Mounter.IsLikelyNotMountPoint(mountpoint); err != nil || !notMnt {
		_ = exec.Command(r.Umount, "-uz", mountpoint).Run()
	}
	args := append([]string{"--pool", pool, "--container", container, "--mountpoint", mountpoint, "--foreground"}, r.Args...)
	cmd := exec.Command(r.Bin, args...)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start dfuse: %w", err)
	}
	r.mu.Lock()
	r.procs[mountpoint] = cmd
	r.mu.Unlock()
	done := make(chan error, 1)
	go func() {
		err := cmd.Wait()
		r.mu.Lock()
		delete(r.procs, mountpoint)
		r.mu.Unlock()
		klog.InfoS("dfuse exited", "mountpoint", mountpoint, "err", err)
		done <- err
	}()
	// wait until the kernel shows a mount there
	deadline := time.Now().Add(30 * time.Second)
	for {
		notMnt, err := r.Mounter.IsLikelyNotMountPoint(mountpoint)
		if err == nil && !notMnt {
			return nil
		}
		select {
		case err := <-done:
			return fmt.Errorf("dfuse exited before mounting %s: %v", mountpoint, err)
		case <-ctx.Done():
			_ = cmd.Process.Kill()
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			return fmt.Errorf("dfuse did not mount %s within 30s", mountpoint)
		}
	}
}

func (r *DfuseRunner) Stop(_ context.Context, mountpoint string) error {
	// the path can be gone already (a previous unstage, or somebody cleaned up by
	// hand); then there is nothing to unmount and reporting failure would make
	// kubelet retry forever
	if _, err := os.Stat(mountpoint); os.IsNotExist(err) {
		r.mu.Lock()
		delete(r.procs, mountpoint)
		r.mu.Unlock()
		return nil
	}
	out, err := exec.Command(r.Umount, "-u", mountpoint).CombinedOutput()
	if err != nil {
		notMnt, e := r.Mounter.IsLikelyNotMountPoint(mountpoint)
		switch {
		case e == nil && notMnt, os.IsNotExist(e):
			// already unmounted, or the directory went away under us
		default:
			return fmt.Errorf("%s -u %s: %v: %s", r.Umount, mountpoint, err, strings.TrimSpace(string(out)))
		}
	}
	r.mu.Lock()
	cmd := r.procs[mountpoint]
	r.mu.Unlock()
	if cmd != nil && cmd.Process != nil {
		// dfuse exits by itself after the unmount; give it a moment, then insist
		for i := 0; i < 50 && r.Running(mountpoint); i++ {
			time.Sleep(100 * time.Millisecond)
		}
		if r.Running(mountpoint) {
			_ = cmd.Process.Kill()
		}
	}
	return nil
}

func (r *DfuseRunner) Running(mountpoint string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.procs[mountpoint]
	return ok
}

// SystemMounter adapts mount-utils.
type SystemMounter struct{ mount.Interface }

func NewSystemMounter() *SystemMounter { return &SystemMounter{Interface: mount.New("")} }

func (m *SystemMounter) Mount(source, target, fstype string, options []string) error {
	return m.Interface.Mount(source, target, fstype, options)
}
