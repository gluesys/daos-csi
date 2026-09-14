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
	"sync"
)

// fakeCRs plays the operator: containers become Ready on the next Get.
type fakeCRs struct {
	mu         sync.Mutex
	pools      map[string]*PoolStatus
	containers map[string]ContainerSpec
	ready      map[string]bool
	destroyed  map[string]bool
	autoReady  bool
	failReason string
}

func newFakeCRs() *fakeCRs {
	return &fakeCRs{pools: map[string]*PoolStatus{"kv": {Exists: true, UUID: "pool-uuid-1", Label: "kv", SystemRef: "daos", FreeBytes: 100 << 30}},
		containers: map[string]ContainerSpec{}, ready: map[string]bool{}, destroyed: map[string]bool{}, autoReady: true}
}

func key(ns, name string) string { return ns + "/" + name }

func (f *fakeCRs) CreateContainer(_ context.Context, s ContainerSpec) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.containers[key(s.Namespace, s.Name)]; !ok {
		f.containers[key(s.Namespace, s.Name)] = s
	}
	return nil
}

func (f *fakeCRs) GetContainer(_ context.Context, ns, name string) (*ContainerStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.containers[key(ns, name)]; !ok {
		return &ContainerStatus{}, nil
	}
	if f.failReason != "" {
		return &ContainerStatus{Exists: true, Reason: f.failReason, Message: "simulated"}, nil
	}
	if f.autoReady {
		f.ready[key(ns, name)] = true
	}
	if !f.ready[key(ns, name)] {
		return &ContainerStatus{Exists: true, Reason: "CreateInProgress", Message: "daos cont create is running", CapacityBytes: f.containers[key(ns, name)].CapacityBytes}, nil
	}
	return &ContainerStatus{Exists: true, Ready: true, UUID: "cont-uuid-" + name, PoolUUID: "pool-uuid-1", Reason: "Ready", CapacityBytes: f.containers[key(ns, name)].CapacityBytes}, nil
}

func (f *fakeCRs) DeleteContainer(_ context.Context, ns, name string, destroy bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.containers, key(ns, name))
	f.destroyed[key(ns, name)] = destroy
	return nil
}

func (f *fakeCRs) GetPool(_ context.Context, name string) (*PoolStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if p, ok := f.pools[name]; ok {
		return p, nil
	}
	return &PoolStatus{}, nil
}

// fakeMounts records bind mounts; fakeFuse records dfuse processes.
type fakeMounts struct {
	mu     sync.Mutex
	mounts map[string]string // target -> source
}

func newFakeMounts() *fakeMounts { return &fakeMounts{mounts: map[string]string{}} }

func (m *fakeMounts) Mount(source, target, _ string, _ []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mounts[target] = source
	return nil
}

func (m *fakeMounts) Unmount(target string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.mounts[target]; !ok {
		return fmt.Errorf("%s not mounted", target)
	}
	delete(m.mounts, target)
	return nil
}

func (m *fakeMounts) IsLikelyNotMountPoint(file string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.mounts[file]
	return !ok, nil
}

type fakeFuse struct {
	mu      sync.Mutex
	running map[string][2]string // mountpoint -> pool, container
	starts  int
	fail    bool
}

func newFakeFuse() *fakeFuse { return &fakeFuse{running: map[string][2]string{}} }

func (f *fakeFuse) Start(_ context.Context, pool, container, mountpoint string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return fmt.Errorf("dfuse: DER_NONEXIST")
	}
	f.starts++
	f.running[mountpoint] = [2]string{pool, container}
	return nil
}

func (f *fakeFuse) Stop(_ context.Context, mountpoint string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.running, mountpoint)
	return nil
}

func (f *fakeFuse) Running(mountpoint string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.running[mountpoint]
	return ok
}
