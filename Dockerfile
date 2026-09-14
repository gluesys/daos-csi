# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Gluesys Co., Ltd.
#
# daos-csi: the CSI driver on top of the daos-client image, which provides
# dfuse and fusermount3 (node mode). Controller mode uses the same image.
ARG BASE=registry.gitlab.gluesys.com/exastor/daos-images/daos-client:2.8.0-20260914
ARG BUILD_IMAGE=golang:1.26

FROM ${BUILD_IMAGE} AS builder
ENV GOTOOLCHAIN=auto GOFLAGS=-buildvcs=false
WORKDIR /workspace
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ cmd/
COPY pkg/ pkg/
RUN CGO_ENABLED=0 GOOS=linux go build -a -o daos-csi ./cmd/daos-csi && CGO_ENABLED=0 GOOS=linux go build -a -o csi-check ./cmd/csi-check

FROM ${BASE}
COPY --from=builder /workspace/daos-csi /workspace/csi-check /usr/local/bin/
RUN mkdir -p /csi /var/lib/daos-csi && chmod 0755 /usr/local/bin/daos-csi
ENTRYPOINT ["/usr/local/bin/daos-csi"]
