<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 Gluesys Co., Ltd. -->

# daos-csi

CSI driver for DAOS. node 플러그인은 dfuse 로 RWX PV 를 마운트하고, controller 는 `DaosContainer`
생성 = PV 로 대응한다. StorageClass 파라미터로 oclass / chunk-size / rd_fac 을 노출한다
(lmcache-daos 실측 기본값: chunk 4 MiB, `RP_2GX`).

이 저장소가 지키는 규칙(모든 exastor K8s 저장소 공통):
1. **CRD 가 유일한 관리 API.** 어플라이언스 REST/UI 와 코드를 공유하지 않는다.
2. **두 번째 SSoT 를 만들지 않는다.** 원하는 상태 = CR spec, 실제 상태 = DAOS MS DB·메트릭. operator 는 비교만 한다.
3. **파괴적 작업 자동화 금지.** `storage format`/wipe/재포맷은 사람 승인(어노테이션) 없이 실행하지 않는다.
4. **upstream-first.** 패치는 먼저 daos-stack / ai-dynamo/nixl / LMCache 로 보낸다.

## 상태
- **Phase 2 #2·#1·#3 (2026-09-15)**: Go 드라이버 `cmd/daos-csi`(`--mode controller|node|all`).
  - Controller: `CreateVolume` → `DaosContainer` CR(PV 이름 = CR 이름 = 볼륨 ID) 생성 후 operator 가 `status.ready` 를 올릴 때까지 대기
    (예산 초과 시 `Unavailable` 로 provisioner 재시도). `DeleteVolume` → `destroy-approved` 어노테이션 + CR 삭제(`destroyOnDelete=false` 면 orphan).
    StorageClass 파라미터: `pool`(필수), `oclass`, `dirOclass`, `chunkSize`, `rdFac`, `csum`, `namespace`, `property.<k>`.
  - Node: 볼륨당 dfuse 프로세스(ADR-csi-001), staging/target bind, 상태 파일로 재시작 복구. block 볼륨 미지원.
  - `kubernetes-csi/csi-test` sanity 가 가짜 마운터·CR 클라이언트로 통과한다(`go test ./...`).
- 뼈대는 kubernetes-csi `csi-driver-host-path` 구조를 따른다. 선행 사례(tkokamo/csi-driver-daos, saikat-royc/daos-csi-driver)는 참고만.
- operator CRD 는 unstructured 로 다룬다(비공개 모듈 의존 회피). 필드 계약: `daos.gluesys.com/v1alpha1 DaosContainer{spec.poolRef,label,type,
  fileOclass,dirOclass,chunkSize,redundancyFactor,checksum,properties; status.uuid,poolUUID,ready,conditions[Ready]}`, `DaosPool{spec.systemRef; status.uuid,label,freeBytes}`.

## 사용
```bash
make build            # bin/daos-csi
make test             # 단위 + csi-sanity
make image            # exastor/daos-csi/daos-csi (daos-client 베이스: dfuse, fusermount3)
kubectl apply -k deploy/   # CSIDriver, controller Deployment(csi-provisioner), node DaemonSet(registrar + agent 사이드카), StorageClass 예시
```
StorageClass 예시(`deploy/storageclass.yaml`): `provisioner: daos.csi.gluesys.com`, `parameters: {pool: kv, oclass: RP_2GX, chunkSize: "4194304", csum: crc32}`,
`reclaimPolicy: Delete`. PVC 는 RWX/RWO 모두 같은 dfuse 마운트다.

## 알려진 제한 (Phase 2)
- 노드 플러그인 재시작·업그레이드 중 그 노드의 DAOS PV I/O 가 끊긴다(FUSE). 상태 파일로 자동 재마운트한다.
- 용량은 컨테이너가 아니라 풀에서 강제된다(DAOS 2.8). `CreateVolume` 은 풀 free 보다 큰 요청만 거부한다.
- 노드 플러그인은 hostNetwork·privileged·`/dev` 마운트(fabric, FUSE). RDMA 디바이스 플러그인 전환은 operator 와 함께.
