# 런타임 데이터 운영규칙 (`docs/runtime-data-policy.md`)

## 목적
이 문서는 AWS에서 GitHub 비공유 자료를 어떻게 생성, 저장, 보관, 백업, 정리할지에 대한 운영 규칙을 정의한다.
이 문서는 단순 설명 문서가 아니라, 모든 패치가 따라야 하는 저장 계약(storage contract)이다.

---

## 1. 적용 범위
이 문서는 다음 자료에 적용된다.
- 실제 env 파일
- SQLite DB
- run lock / pid / heartbeat
- 로그
- 주문/체결/잔고/리컨실 기록
- 전략 검증/관측/튜닝 산출물
- healthcheck/ops report/incident snapshot
- backup/snapshot/archive

적용 대상에서 제외되는 것은 다음뿐이다.
- GitHub 레포 내 코드, 테스트, 문서, 템플릿

---

## 2. 최고 원칙

### 2.1 레포 밖 저장 원칙
운영 산출물은 GitHub 레포 밖에 저장한다.

예외 없음.

금지 예시:
- 레포 내부 `data/*.sqlite`
- 레포 내부 `backups/*`
- 레포 내부 `*.log`
- 레포 내부 `tmp/*`

### 2.2 환경 분리 원칙
`paper`, `live`는 서로 다른 저장소를 사용한다.
다음을 절대 공유하지 않는다.
- DB_PATH
- RUN_LOCK_PATH
- BACKUP_DIR
- report output
- audit / error logs

`MODE=live`에서는 `DATA_ROOT`, `LOG_ROOT`, `BACKUP_ROOT`를 반드시 **레포 외부 절대경로**로 지정한다.
상대경로, 레포 내부 경로, `paper` 세그먼트가 섞인 경로는 운영 정책 위반이다.

### 2.3 경로 주입 원칙
실제 저장 위치는 환경변수나 env 파일에서 주입한다.
코드가 절대경로를 하드코딩하지 않는다.

### 2.4 경로 중앙화 원칙
새 코드가 파일을 쓰거나 읽을 때는 공용 경로 처리 계층을 통해야 한다.
직접 문자열로 경로를 조합하지 않는다.

---

## 3. 운영자가 관리하는 것 vs 코드가 관리하는 것

### 운영자가 관리하는 것
- `RUNTIME_ROOT`
- `DATA_ROOT`
- `RUN_ROOT`
- `LOG_ROOT`
- `BACKUP_ROOT`
- 실제 `DB_PATH`
- 실제 `RUN_LOCK_PATH`
- 실제 secret / webhook / API key

### 코드가 관리하는 것
- 하위 디렉토리 표준
- 파일명 규칙
- 날짜별 회전 규칙
- JSONL/SQLite 저장 형식
- mode 기반 분기 규칙
- 디렉토리 자동 생성 로직

한 줄로 정리하면:

> 경로는 설정, 규칙은 코드.

---

## 4. 데이터 분류 규칙

### 4.1 env
- 실제 비밀값이 포함된다.
- GitHub 금지
- AWS 권한 제한 필요
- 백업 시 redacted copy만 남기는 것을 권장

### 4.2 run
- pid, lock, heartbeat, temp state pointer
- 프로그램이 종료되면 의미가 사라질 수 있음
- 복구용 힌트는 가능하지만 영구 원장 아님

### 4.3 raw
- 거래소/브로커 원본 응답
- 원문 보존이 목적
- 분석용 가공 금지
- 민감 필드 마스킹 규칙 필요

### 4.4 derived
- 지표, feature, validation, tuning intermediate
- raw를 덮어쓰지 않고 별도 저장
- market catalog snapshot(정규화된 마켓/경고 상태 스냅샷)은 `data/<mode>/derived/market_catalog_snapshot/`에 저장한다.

### 4.5 trades
- 주문 요청/응답
- 체결
- 잔고/포지션 스냅샷
- reconcile 결과
- 운영 핵심 자산
- 장기 보존 우선

### 4.6 reports
- ops report
- strategy report
- fee diagnostics
- incident summary
- 사람이 읽는 운영 요약
- market catalog diff 이벤트(JSONL append-only)는 `data/<mode>/reports/market_catalog_diff/`에 저장한다.

### 4.7 logs
- app
- strategy
- orders
- fills
- errors
- audit

### 4.8 backup
- DB snapshot
- redacted config snapshot
- incident runtime snapshot
- 장기 복구용 자산

---

## 5. 저장 형식 규칙

### 5.1 SQLite 사용 대상
다음은 SQLite 원장 또는 상태 저장을 허용한다.
- candles
- portfolio
- orders
- fills
- bot_health
- trade_lifecycles
- 기타 상태 복구 핵심 테이블

### 5.2 JSONL 사용 대상
다음은 JSONL append-only를 권장한다.
- 주문 요청 이벤트
- 주문 응답 이벤트
- 체결 이벤트
- balance snapshot
- reconcile summary
- strategy decision evidence
- external raw response snapshot

### 5.3 overwrite 금지 대상
다음은 append-only 또는 snapshot 방식만 허용한다.
- live 주문/체결 관련 기록
- live balance snapshot
- audit / incident evidence
- strategy decision evidence

---

## 6. 로그 규칙

### 6.1 최소 로그 분리
다음을 최소 분리 단위로 본다.
- app
- strategy
- orders
- fills
- errors
- audit

### 6.2 로그 보존 기준
- 운영 중 실시간 확인은 stdout/journald 사용 가능
- 그러나 개념상 로그 분류 기준은 반드시 유지한다.
- 장기 보존 대상은 `errors`, `audit`, 주요 `orders/fills` 관련 이벤트 우선

### 6.3 로그에 남기면 안 되는 것
- API secret
- webhook secret
- full auth header
- 전체 민감 payload 원문

민감정보가 필요한 경우 redaction 후 저장한다.

---

## 7. 전략 검증/관측 데이터 규칙

향후 전략 인프라 패치에서 다음 자료가 새로 생길 수 있다.
- signal trace
- decision context
- blocked reason
- feature snapshot
- validation result
- tuning result
- experiment metadata

이 자료는 다음처럼 분류한다.
- 원본 응답: `data/<mode>/raw/`
- 가공 특징량/검증 산출물: `data/<mode>/derived/`
- 사람이 읽는 보고서: `data/<mode>/reports/`
- 실제 주문/체결 결과와 연결되는 근거 로그: `data/<mode>/trades/` 또는 `logs/<mode>/audit/`

중요 규칙:
- 전략 튜닝 산출물은 live 체결 원장과 같은 파일에 섞지 않는다.
- 실전 판단 근거와 실험용 디버그 덤프를 분리한다.

---

## 8. 백업 규칙

### 8.1 백업 대상 우선순위
P0:
- live DB
- live env redacted snapshot
- live error/audit evidence

P1:
- paper DB
- strategy reports
- validation summaries

P2:
- raw market cache
- 재생성 가능한 derived data

### 8.2 백업 위치
- 1차: `BACKUP_ROOT/<mode>/...`
- 2차: S3 또는 외부 안전 저장소 이관 권장

### 8.3 복구 검증
- 백업은 생성만으로 끝내지 않는다.
- restore verify 절차를 주기적으로 수행한다.

---

## 9. 파일 생성 규칙

### 9.1 날짜 기준
- 일별 파일 기본
- 기준 시간대는 KST

### 9.2 이름 규칙
- 소문자, 숫자, `_`, `-`만 사용
- 공백 금지
- 확장자로 자료 성격을 드러낸다 (`.jsonl`, `.log`, `.sqlite`, `.txt`, `.json`)

### 9.3 임시 파일 규칙
임시 파일도 아무 데나 두지 않는다.
허용 위치:
- `run/<mode>/tmp/`
- `data/<mode>/derived/tmp/`

---

## 10. 패치 규칙

새 패치를 만들 때는 기능 설명만으로 부족하다.
다음 저장 영향 항목을 함께 명시해야 한다.

1. 새로 저장되는 자료가 있는가
2. 어느 환경에 저장되는가
3. 어느 분류에 속하는가
4. 파일/DB/로그 중 무엇인가
5. append-only인가 overwrite인가
6. 민감정보 포함 여부는 무엇인가
7. 백업 대상인가
8. retention 규칙이 필요한가

이 항목이 없으면 패치 리뷰를 통과하지 않는다.

---

## 11. 코드 규칙

### 11.1 금지
- `Path("data/..." )` 직접 사용
- `./backups`, `./logs`, `./tmp` 직접 사용
- repo 루트 기준 상대경로를 운영 기본값으로 삼는 것
- 모듈마다 제각각 파일명을 만드는 것

### 11.2 허용
- 설정으로 주입된 root 경로 사용
- 공용 PathManager/StorageManager를 통한 경로 획득
- paper/dryrun에서만 제한적 로컬 개발 경로 사용
- live에서는 우회 규칙 없이 명시적 절대경로만 허용

### 11.3 live 추가 규칙
- `DB_PATH` 명시 필수
- `RUN_LOCK_PATH`는 `RUN_ROOT/live/` 하위 절대경로를 사용
- `ENV_ROOT/RUN_ROOT/DATA_ROOT/LOG_ROOT/BACKUP_ROOT`는 모두 절대경로 + 레포 외부 + mode 혼합 금지
- notifier 미설정 상태 live 실행 금지
- live에서 paper 전용 env 키 사용 금지

---

## 12. 운영 점검 규칙

배포 전 체크:
- live env와 paper env가 분리되었는가
- live DB 경로가 레포 밖 절대경로인가
- run lock이 `run/live/`에 위치하는가
- backup 경로가 레포 밖 절대경로인가
- healthcheck / backup / main service가 같은 live env를 보는가

패치 후 체크:
- 새 로그/데이터가 어느 분류에 들어가는지 확인
- 임시 디버그 파일이 레포 안에 생기지 않는지 확인
- paper/live 혼합 경로가 새로 생기지 않는지 확인
- 백업 대상이 늘었으면 문서 업데이트

운영 중 체크:
- 전일 orders/fills/errors/audit 파일 존재 확인
- backup 최근 생성 확인
- restore verify 최근 성공 확인
- incident snapshot 경로 일관성 확인

---

## 13. 문서 변경 규칙

다음 중 하나가 생기면 이 문서를 갱신한다.
- 새 저장 분류 추가
- 새 경로 root 추가
- 새 백업 규칙 추가
- 전략 검증 산출물 타입 추가
- API 응답 raw 저장 정책 변경
- 민감정보 redaction 규칙 변경

---

## 14. 현재 코드베이스에 대한 적용 메모

현재 코드베이스는 이 문서를 "권고"가 아닌 계약으로 취급해야 한다.
- live에서 `DB_PATH` 명시가 강제되어야 한다.
- live에서 운영 루트(`ENV_ROOT/RUN_ROOT/DATA_ROOT/LOG_ROOT/BACKUP_ROOT`)는 절대경로 + 레포 외부여야 한다.
- live에서 `paper` 세그먼트가 섞인 경로는 거부해야 한다.
- 스크립트/코드/테스트/문서는 동일한 PathManager 규칙을 사용해야 한다.

---

## 15. 최종 원칙

> 운영 구조가 먼저이고, 코드는 그 운영 구조를 깨지 않도록 만들어져야 한다.

이 문서는 그 운영 구조를 정의하는 기준 문서다.
