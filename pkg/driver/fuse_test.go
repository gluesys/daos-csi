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
	"sync"
	"syscall"
	"testing"
	"time"
)

// A dfuse that never mounts must leave nothing behind: the killed process stayed
// in procs until the reaper goroutine ran, so the next Start took the mount for
// granted and the driver published a directory with no DAOS behind it
// (exaci4-2, 2026-09-22).
func TestDfuseRunnerForgetsAProcessThatNeverMounted(t *testing.T) {
	dir := t.TempDir()
	// a "dfuse" that runs but never mounts anything, whatever arguments it gets
	fake := filepath.Join(dir, "fake-dfuse")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\nexec sleep 5\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	r := NewDfuseRunner(neverMounted{})
	r.Bin, r.Timeout = fake, 300*time.Millisecond
	mnt := filepath.Join(dir, "m")

	if err := r.Start(context.Background(), "p", "c", mnt); err == nil {
		t.Fatal("Start must fail when dfuse never mounts")
	}
	if r.Running(mnt) {
		t.Fatal("a dfuse that never mounted must not stay tracked")
	}
	if err := r.Start(context.Background(), "p", "c", mnt); err == nil {
		t.Fatal("the retry must run dfuse again, not report the old process as mounted")
	}
}

// neverMounted answers "nothing is mounted there", like a mountpoint dfuse never
// reached.
type neverMounted struct{}

func (neverMounted) Mount(_, _, _ string, _ []string) error     { return nil }
func (neverMounted) Unmount(string) error                       { return nil }
func (neverMounted) IsLikelyNotMountPoint(string) (bool, error) { return true, nil }

// A dfuse that died leaves a mountpoint where every stat fails with ENOTCONN, so
// MkdirAll there fails with "file exists". Clearing the stale mount has to come
// first or the volume can never be staged again (exaci4-2, 2026-09-23).
func TestDfuseRunnerClearsAStaleMountBeforeCreatingTheDirectory(t *testing.T) {
	dir := t.TempDir()
	mnt := filepath.Join(dir, "m")
	// stands in for the unusable path a dead FUSE mount leaves behind
	if err := os.Symlink(filepath.Join(dir, "gone"), mnt); err != nil {
		t.Fatal(err)
	}
	umount := filepath.Join(dir, "fake-fusermount3")
	if err := os.WriteFile(umount, []byte("#!/bin/sh\nrm -f \"$2\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	fake := filepath.Join(dir, "fake-dfuse")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\nexec sleep 5\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	r := NewDfuseRunner(deadMount{})
	r.Bin, r.Umount, r.Timeout = fake, umount, 200*time.Millisecond

	// it still fails (the fake dfuse never mounts), but only after getting past
	// the stale path
	if err := r.Start(context.Background(), "p", "c", mnt); err == nil {
		t.Fatal("Start must fail when dfuse never mounts")
	}
	fi, err := os.Stat(mnt)
	if err != nil || !fi.IsDir() {
		t.Fatalf("the stale mount must be cleared and the directory created: %v", err)
	}
}

// deadMount answers like a mountpoint whose FUSE daemon is gone.
type deadMount struct{}

func (deadMount) Mount(_, _, _ string, _ []string) error { return nil }
func (deadMount) Unmount(string) error                   { return nil }
func (deadMount) IsLikelyNotMountPoint(p string) (bool, error) {
	return false, &os.PathError{Op: "stat", Path: p, Err: syscall.ENOTCONN}
}

// Startup recovery and a NodeStage for the same volume can race; only one
// dfuse may be spawned at a time for a mountpoint.
func TestDfuseRunnerSerializesStartsPerMountpoint(t *testing.T) {
	dir := t.TempDir()
	log := filepath.Join(dir, "starts.log")
	fake := filepath.Join(dir, "fake-dfuse")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\ndate +%s%N >> "+log+"\nexec sleep 5\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	r := NewDfuseRunner(neverMounted{})
	r.Bin, r.Timeout = fake, 200*time.Millisecond
	mnt := filepath.Join(dir, "m")
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = r.Start(context.Background(), "p", "c", mnt) }()
	}
	wg.Wait()
	b, _ := os.ReadFile(log)
	lines := strings.Fields(string(b))
	if len(lines) != 3 {
		t.Fatalf("expected 3 sequential spawns, got %d", len(lines))
	}
	for i := 1; i < len(lines); i++ {
		var a, c int64
		fmt.Sscan(lines[i-1], &a)
		fmt.Sscan(lines[i], &c)
		if c-a < int64(150*time.Millisecond) {
			t.Fatalf("spawns %d and %d overlapped (%d ms apart)", i-1, i, (c-a)/1e6)
		}
	}
}
