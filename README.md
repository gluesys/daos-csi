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
Phase 0. 설계 노트만(`doc/design.md`). 뼈대는 kubernetes-csi `csi-driver-host-path` 구조를 따른다.
선행 사례: tkokamo/csi-driver-daos(2021, DAOS 1.2, dfuse), saikat-royc/daos-csi-driver(2023, 미완).
둘 다 참고만 하고 코드는 새로 쓴다.
