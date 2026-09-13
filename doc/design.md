<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 Gluesys Co., Ltd. -->

# daos-csi 설계 노트 (Phase 0)

## 범위
- Controller: `CreateVolume` → `DaosContainer` CR 생성(operator 가 실제 `daos cont create`), `DeleteVolume` → CR 삭제.
  볼륨 ID = `<pool-uuid>/<cont-uuid>`. 용량은 컨테이너 속성이 아니라 풀 quota 로 강제(2.8 기준 컨테이너 크기 제한 없음).
- Node: `NodePublishVolume` 은 dfuse(`--pool --container --mountpoint`) foreground 프로세스를 파드별로 띄우고 bind mount.
  agent 소켓은 `/var/run/daos_agent` hostPath(daos-agent DaemonSet 이 제공).
- 접근 모드: RWX(ReadWriteMany) 우선. RWO 는 동일 경로.
- StorageClass 파라미터: `pool`, `oclass`(기본 RP_2GX), `chunkSize`(기본 4194304), `rdFac`, `csum`(기본 crc32, 2.8 기본값과 일치).

## 미결
- dfuse 프로세스 수명: 노드 플러그인 재시작 시 마운트 유지 방법(FUSE 특성상 데몬 재기동 = 마운트 소실). 후보: 파드별 dfuse 사이드카 대신 노드 플러그인이 systemd-run 으로 호스트에 위임.
- 2.6.4 의 `--dump-handles/--read-handles` 로 대량 마운트 시 서버 pool connect 병목 완화 여부.
- 인증: agent 가 UNIX 소켓 UID 로 사용자를 식별하므로 파드 UID 매핑 정책 필요.
