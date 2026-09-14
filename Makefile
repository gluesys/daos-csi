# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Gluesys Co., Ltd.
IMAGE_REGISTRY ?= registry.gitlab.gluesys.com/exastor/daos-csi
IMAGE_TAG ?= $(shell git describe --always --dirty 2>/dev/null || echo dev)
IMG ?= $(IMAGE_REGISTRY)/daos-csi:$(IMAGE_TAG)
BASE ?= registry.gitlab.gluesys.com/exastor/daos-images/daos-client:2.8.0-20260914
DOCKER ?= docker
export GOTOOLCHAIN ?= auto
export GOFLAGS ?= -buildvcs=false

.PHONY: build test fmt vet image push
build: ## Build bin/daos-csi
	CGO_ENABLED=0 go build -o bin/daos-csi ./cmd/daos-csi
test: fmt vet ## Unit tests + csi-sanity
	go test ./... -coverprofile cover.out
fmt:
	gofmt -l cmd pkg | tee /tmp/gofmt.txt; test ! -s /tmp/gofmt.txt
vet:
	go vet ./...
image: ## Build the driver image (daos-client base for dfuse)
	$(DOCKER) build --build-arg BASE=$(BASE) -t $(IMG) .
push:
	$(DOCKER) push $(IMG)
