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
	"strconv"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

// The driver reads and writes the operator's CRDs as unstructured objects so
// it has no build dependency on exastor/daos-operator (a private module). The
// field names below are the API contract with daos.gluesys.com/v1alpha1.
var (
	gvrContainer = schema.GroupVersionResource{Group: "daos.gluesys.com", Version: "v1alpha1", Resource: "daoscontainers"}
	gvrPool      = schema.GroupVersionResource{Group: "daos.gluesys.com", Version: "v1alpha1", Resource: "daospools"}
)

const (
	// LabelVolume marks DaosContainers owned by this driver (value = volume id).
	LabelVolume = "csi.daos.gluesys.com/volume"
	// AnnotationCapacity records the requested size so a retry with a different
	// size can be rejected (CSI idempotency rule); DAOS itself has no per-container quota.
	AnnotationCapacity = "csi.daos.gluesys.com/capacity-bytes"
	// AnnotationDestroyApproved is the operator's gate for actually destroying the
	// DAOS container when the CR is deleted; a PV delete (reclaimPolicy Delete) is
	// exactly that decision, made by the storage admin through the StorageClass.
	AnnotationDestroyApproved = "daos.gluesys.com/destroy-approved"
)

// ContainerSpec is what CreateVolume writes.
type ContainerSpec struct {
	Namespace, Name string
	PoolRef         string
	Label           string
	Type            string
	FileOclass      string
	DirOclass       string
	ChunkSize       int64
	RedundancyFac   *int32
	Checksum        string
	Properties      map[string]string
	VolumeID        string
	Labels          map[string]string
	CapacityBytes   int64
}

// ContainerStatus is what the driver reads back.
type ContainerStatus struct {
	Exists   bool
	UUID     string
	PoolUUID string
	Ready    bool
	Message  string // Ready condition message
	Reason   string
	// CapacityBytes is what the volume was created with (annotation), 0 if unknown.
	CapacityBytes int64
}

// PoolStatus is the subset of DaosPool the driver needs.
type PoolStatus struct {
	Exists    bool
	UUID      string
	Label     string
	SystemRef string
	FreeBytes int64
}

// ContainerClient abstracts the CRD access for tests.
type ContainerClient interface {
	CreateContainer(ctx context.Context, spec ContainerSpec) error
	GetContainer(ctx context.Context, ns, name string) (*ContainerStatus, error)
	DeleteContainer(ctx context.Context, ns, name string, destroy bool) error
	GetPool(ctx context.Context, name string) (*PoolStatus, error)
}

// DynamicClient is the real ContainerClient.
type DynamicClient struct{ Dyn dynamic.Interface }

func (c *DynamicClient) CreateContainer(ctx context.Context, s ContainerSpec) error {
	spec := map[string]any{"poolRef": s.PoolRef}
	if s.Label != "" {
		spec["label"] = s.Label
	}
	if s.Type != "" {
		spec["type"] = s.Type
	}
	if s.FileOclass != "" {
		spec["fileOclass"] = s.FileOclass
	}
	if s.DirOclass != "" {
		spec["dirOclass"] = s.DirOclass
	}
	if s.ChunkSize > 0 {
		spec["chunkSize"] = s.ChunkSize
	}
	if s.RedundancyFac != nil {
		spec["redundancyFactor"] = int64(*s.RedundancyFac)
	}
	if s.Checksum != "" {
		spec["checksum"] = s.Checksum
	}
	if len(s.Properties) > 0 {
		p := map[string]any{}
		for k, v := range s.Properties {
			p[k] = v
		}
		spec["properties"] = p
	}
	labels := map[string]any{LabelVolume: s.VolumeID, "app.kubernetes.io/managed-by": "daos-csi"}
	for k, v := range s.Labels {
		labels[k] = v
	}
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": gvrContainer.Group + "/" + gvrContainer.Version, "kind": "DaosContainer",
		"metadata": map[string]any{"name": s.Name, "namespace": s.Namespace, "labels": labels,
			"annotations": map[string]any{AnnotationCapacity: strconv.FormatInt(s.CapacityBytes, 10)}},
		"spec": spec,
	}}
	_, err := c.Dyn.Resource(gvrContainer).Namespace(s.Namespace).Create(ctx, obj, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func (c *DynamicClient) GetContainer(ctx context.Context, ns, name string) (*ContainerStatus, error) {
	u, err := c.Dyn.Resource(gvrContainer).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return &ContainerStatus{}, nil
	}
	if err != nil {
		return nil, err
	}
	st := &ContainerStatus{Exists: true}
	st.UUID, _, _ = unstructured.NestedString(u.Object, "status", "uuid")
	st.PoolUUID, _, _ = unstructured.NestedString(u.Object, "status", "poolUUID")
	st.Ready, _, _ = unstructured.NestedBool(u.Object, "status", "ready")
	if v := u.GetAnnotations()[AnnotationCapacity]; v != "" {
		st.CapacityBytes, _ = strconv.ParseInt(v, 10, 64)
	}
	conds, _, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
	for _, c := range conds {
		m, ok := c.(map[string]any)
		if ok && m["type"] == "Ready" {
			st.Message, _ = m["message"].(string)
			st.Reason, _ = m["reason"].(string)
		}
	}
	return st, nil
}

func (c *DynamicClient) DeleteContainer(ctx context.Context, ns, name string, destroy bool) error {
	res := c.Dyn.Resource(gvrContainer).Namespace(ns)
	if destroy {
		u, err := res.Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		ann := u.GetAnnotations()
		if ann[AnnotationDestroyApproved] != "true" {
			if ann == nil {
				ann = map[string]string{}
			}
			ann[AnnotationDestroyApproved] = "true"
			u.SetAnnotations(ann)
			if _, err := res.Update(ctx, u, metav1.UpdateOptions{}); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}
	err := res.Delete(ctx, name, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func (c *DynamicClient) GetPool(ctx context.Context, name string) (*PoolStatus, error) {
	u, err := c.Dyn.Resource(gvrPool).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return &PoolStatus{}, nil
	}
	if err != nil {
		return nil, err
	}
	p := &PoolStatus{Exists: true}
	p.UUID, _, _ = unstructured.NestedString(u.Object, "status", "uuid")
	p.Label, _, _ = unstructured.NestedString(u.Object, "status", "label")
	p.SystemRef, _, _ = unstructured.NestedString(u.Object, "spec", "systemRef")
	p.FreeBytes, _, _ = unstructured.NestedInt64(u.Object, "status", "freeBytes")
	if p.Label == "" {
		p.Label = name
	}
	return p, nil
}

// String is for logs.
func (s ContainerStatus) String() string {
	return fmt.Sprintf("exists=%t ready=%t uuid=%s reason=%s", s.Exists, s.Ready, s.UUID, s.Reason)
}
