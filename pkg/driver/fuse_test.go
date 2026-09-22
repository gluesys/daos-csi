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
	"os"
	"path/filepath"
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
