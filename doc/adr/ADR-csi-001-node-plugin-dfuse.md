<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 Gluesys Co., Ltd. -->

# ADR-csi-001: 노드 플러그인 — 볼륨당 dfuse 프로세스 + 상태 파일 복구, 사이드카 agent

- 상태: 제안 (이슈 #1)
- 날짜: 2026-09-15

## 배경
dfuse 는 FUSE 데몬이라 프로세스가 죽으면 마운트가 사라진다. 노드 플러그인(DaemonSet) 이 재시작·업그레이드될 때 파드가 쓰고 있는
PV 마운트가 끊기는 문제를 어떻게 다룰지 정해야 한다(`doc/design.md` 미결). 후보:

1. **노드 플러그인 안에서 볼륨당 dfuse 를 자식 프로세스로 실행**하고, 볼륨 상태를 hostPath 에 기록해 재시작 시 다시 띄운다.
2. `systemd-run` 으로 호스트에 dfuse 를 위임 — 플러그인 재시작과 무관하게 마운트 유지.
3. 볼륨당 "mount pod"(JuiceFS CSI 방식) — 플러그인과 dfuse 수명을 분리하고 격리도 높다.

## 결정
**1 을 Phase 2 구현으로 채택**하고, 3 은 Phase 3 후보로 남긴다. 2 는 채택하지 않는다.

- `NodeStageVolume` = 드라이버 소유 경로 `/var/lib/daos-csi/mounts/<volume>` 에 dfuse(`--pool --container --mountpoint --foreground`) 를
  띄우고, 그 경로를 kubelet 의 staging 경로에 bind. `NodePublishVolume` = staging → 파드 target bind(ro 지원).
  pool/container 는 volume context 의 **UUID** 를 우선 사용한다(레이블 변경에 안전).
- 상태 파일 `/var/lib/daos-csi/state/<volume>.json`(pool, container, mount). 플러그인 기동 시 이를 읽어 dfuse 를 다시 띄운다.
  stale FUSE 마운트는 `fusermount3 -uz` 로 먼저 정리한다. **재시작 중에는 I/O 가 끊긴다**(ENOTCONN) — 문서화한 제한.
- `daos_agent` 는 노드 플러그인 파드의 네이티브 사이드카(`initContainers[].restartPolicy: Always`) 로, operator 가 만든
  `<sys>-agent` ConfigMap 과 (TLS 시) `<sys>-certs` 의 agent 세트를 마운트한다. 소켓은 파드 내 emptyDir. 호스트 agent 는 필요 없다.
- 볼륨 ID = `DaosContainer` CR 이름(= PV 이름 `pvc-<uid>`). 설계 노트의 `pool-uuid/cont-uuid` 대신 택한 이유: CreateVolume 은 첫 호출에서
  ID 를 돌려줘야 하고 UUID 는 operator 가 컨테이너를 만든 뒤에야 생긴다. UUID 는 volume context 로 전달한다.

## 근거
- 2 는 호스트에 DAOS 클라이언트 라이브러리·systemd 의존을 만들고, 컨테이너 이미지로 배포한다는 전제(exastor/daos-images)에 어긋난다.
- 3 은 옳은 방향이지만 mount pod 스케줄링·GC·업그레이드 조율이 필요해 Phase 2 종료 기준(helm install → 15분 내 PV 마운트)에 과하다.
- 1 은 kubernetes-csi 의 여러 FUSE 드라이버(seaweedfs-csi 등)가 쓰는 방식이고, 상태 파일 복구로 "재시작 = 영구 소실" 은 피한다.

## 결과와 트레이드오프
- 노드 플러그인 업그레이드는 그 노드의 DAOS PV I/O 를 잠시 끊는다. DaemonSet `maxUnavailable: 1` + 재시작 시 복구로 창을 줄인다.
- 파드 UID 매핑: dfuse 는 노드 플러그인(root) 의 UID 로 돌고 agent 는 그 UID 로 사용자를 식별한다. 컨테이너 ACL 은 `A::OWNER@:rwdtTaAo`
  기본이라 root 마운트로 접근된다. 사용자별 ACL 은 Phase 3.

## 재검토 조건
mount pod 방식으로 갈 때(Phase 3), 또는 DAOS 가 dfuse 핸들 인계(`--dump-handles/--read-handles`)를 안정화할 때.
