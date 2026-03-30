# AWS 비공유 자료 구조 제안 (`docs/storage-layout.md`)

## 목적
이 문서는 GitHub 레포 밖에서 관리해야 하는 AWS 운영 자료의 표준 경로를 정의한다.
핵심 목표는 다음 4가지다.

1. `paper` / `live` 환경을 절대 섞지 않는다.
2. 코드 저장소와 런타임 산출물을 분리한다.
3. 장애 복구와 원인 추적이 가능한 구조를 만든다.
4. 이후 어떤 패치가 들어와도 동일한 저장 규칙을 유지한다.

---

## 1. 최상위 원칙

### 1.1 레포와 운영 자료 분리
- GitHub 레포에는 코드, 테스트, 문서, 템플릿만 둔다.
- 실제 운영 산출물은 레포 밖 AWS 전용 디렉토리에 둔다.
- 금지 예시:
  - `repo/data/*.sqlite`
  - `repo/backups/*`
  - `repo/*.log`
  - `repo/tmp/*`

### 1.2 환경 완전 분리
- 최소 분리 환경: `paper`, `live`
- 필요 시 `dryrun` 추가 가능하되, 처음 구조부터 확장 가능하게 설계한다.
- 각 환경은 다음을 공유하지 않는다.
  - DB
  - runtime lock/pid
  - logs
  - reports
  - backups

### 1.3 자료 성격별 분리
운영 자료는 다음 기준으로 구분한다.
- `env/`: 실제 운영 env 파일
- `run/`: pid, lock, state pointer 등 런타임 파일
- `data/raw/`: 외부 원본 응답, 원본 시장데이터, 원본 진단 스냅샷
- `data/derived/`: 지표/특징량/검증용 가공 데이터
- `data/trades/`: 주문/체결/잔고/포지션/리컨실 결과
- `data/reports/`: 운영 리포트, 전략 리포트, 수익 검증 산출물
- `logs/`: 사람이 읽는 운영 로그
- `backup/`: DB 스냅샷, 설정 백업, 복구용 아카이브

### 1.4 경로는 설정, 규칙은 코드
- 실제 루트 경로는 env로 주입한다.
- 하위 구조 규칙은 코드가 책임진다.
- 모듈이 직접 `./data`, `./backups`, `./logs` 같은 경로를 만들면 안 된다.

---

## 2. 권장 AWS 운영 루트

권장 루트:

```text
/var/lib/bithumb-bot/
```

대안:

```text
/home/<run-user>/trading-bot-runtime/
```

문서에서는 이하 `RUNTIME_ROOT`로 표기한다.

---

## 3. 권장 디렉토리 구조

```text
RUNTIME_ROOT/
  env/
    paper.env
    live.env

  run/
    paper/
      bithumb-bot.pid
      bithumb-bot.lock
      heartbeat.json
    live/
      bithumb-bot.pid
      bithumb-bot.lock
      heartbeat.json

  data/
    paper/
      raw/
        market/
        broker/
        snapshots/
      derived/
        indicators/
        features/
        validation/
      trades/
        paper.sqlite
        orders/
        fills/
        balances/
        reconcile/
      reports/
        ops/
        strategy/
        pnl/
    live/
      raw/
        market/
        broker/
        snapshots/
      derived/
        indicators/
        features/
        validation/
      trades/
        live.sqlite
        orders/
        fills/
        balances/
        reconcile/
      reports/
        ops/
        strategy/
        pnl/

  logs/
    paper/
      app/
      strategy/
      orders/
      fills/
      errors/
      audit/
    live/
      app/
      strategy/
      orders/
      fills/
      errors/
      audit/

  backup/
    paper/
      db/
      configs/
      snapshots/
    live/
      db/
      configs/
      snapshots/

  archive/
    paper/
    live/
```

---

## 4. 실제 파일 배치 기준

### 4.1 env
```text
RUNTIME_ROOT/env/paper.env
RUNTIME_ROOT/env/live.env
```

- GitHub에는 `.env.example`만 둔다.
- 실제 API key/secret, webhook, 실제 DB 경로는 AWS env 파일에만 둔다.

### 4.2 runtime
```text
RUNTIME_ROOT/run/live/bithumb-bot.lock
RUNTIME_ROOT/run/live/bithumb-bot.pid
RUNTIME_ROOT/run/live/heartbeat.json
```

- `RUN_LOCK_PATH`는 반드시 `run/<mode>/` 아래에 둔다.
- lock 파일을 `data/locks/`에 두는 기존 방식은 중간 단계로는 괜찮지만, 최종 구조로는 `run/`으로 이동하는 것을 권장한다.

### 4.3 DB
```text
RUNTIME_ROOT/data/paper/trades/paper.sqlite
RUNTIME_ROOT/data/live/trades/live.sqlite
```

- 환경별 DB 절대 공유 금지
- live는 반드시 별도 `DB_PATH`를 명시한다.
- `data/bithumb_1m.sqlite` 같은 레포 상대 기본값은 운영 기본값으로 사용하지 않는다.

### 4.4 raw
```text
RUNTIME_ROOT/data/live/raw/market/orderbook_2026-03-30.jsonl
RUNTIME_ROOT/data/live/raw/broker/private_balance_2026-03-30.jsonl
```

- 재현성과 디버깅을 위해 저장하는 원본 응답
- 민감정보가 있으면 redaction 규칙 적용

### 4.5 derived
```text
RUNTIME_ROOT/data/live/derived/validation/signal_trace_2026-03-30.jsonl
RUNTIME_ROOT/data/live/derived/features/features_2026-03-30.parquet
```

- 전략 검증, 관측, 튜닝용 가공 결과
- raw를 덮어쓰지 않는다.

### 4.6 trades
```text
RUNTIME_ROOT/data/live/trades/orders/orders_2026-03-30.jsonl
RUNTIME_ROOT/data/live/trades/fills/fills_2026-03-30.jsonl
RUNTIME_ROOT/data/live/trades/balances/balance_snapshots_2026-03-30.jsonl
RUNTIME_ROOT/data/live/trades/reconcile/reconcile_2026-03-30.jsonl
```

- SQLite는 상태 복구용 원장
- JSONL은 append-only 운영 기록
- 둘 중 하나만 남기는 구조보다, DB + append-only 증거 로그 병행을 권장한다.

### 4.7 reports
```text
RUNTIME_ROOT/data/live/reports/ops/ops_report_2026-03-30T090000KST.txt
RUNTIME_ROOT/data/live/reports/strategy/strategy_report_2026-03-30.json
```

- 임시 터미널 출력도 필요하면 파일 아카이브 가능
- 리포트는 `data/reports/`에 두고, 일반 로그와 분리한다.

### 4.8 logs
```text
RUNTIME_ROOT/logs/live/app/app_2026-03-30.log
RUNTIME_ROOT/logs/live/strategy/strategy_2026-03-30.log
RUNTIME_ROOT/logs/live/orders/orders_2026-03-30.log
RUNTIME_ROOT/logs/live/errors/error_2026-03-30.log
RUNTIME_ROOT/logs/live/audit/audit_2026-03-30.log
```

- stdout/journalctl만으로 운영하지 말고, 최소한 로그 분류 기준은 문서화한다.
- 초기에 journald 중심으로 가더라도 개념상 `app/strategy/orders/fills/errors/audit` 구분을 유지한다.

### 4.9 backup
```text
RUNTIME_ROOT/backup/live/db/live.sqlite.20260330_120000.sqlite
RUNTIME_ROOT/backup/live/configs/live.env.20260330_120000.redacted
RUNTIME_ROOT/backup/live/snapshots/runtime_snapshot_20260330_120000.tar.gz
```

- 백업은 환경별 디렉토리 분리
- `BACKUP_DIR=backups` 같은 레포 상대 기본값은 운영 기본값으로 사용하지 않는다.

---

## 5. 운영 변수 표준안

실제 env에는 최소 다음 경로 변수를 둔다.

```dotenv
MODE=live
RUNTIME_ROOT=/var/lib/bithumb-bot
ENV_ROOT=/var/lib/bithumb-bot/env
RUN_ROOT=/var/lib/bithumb-bot/run
DATA_ROOT=/var/lib/bithumb-bot/data
LOG_ROOT=/var/lib/bithumb-bot/logs
BACKUP_ROOT=/var/lib/bithumb-bot/backup
ARCHIVE_ROOT=/var/lib/bithumb-bot/archive
DB_PATH=/var/lib/bithumb-bot/data/live/trades/live.sqlite
RUN_LOCK_PATH=/var/lib/bithumb-bot/run/live/bithumb-bot.lock
BACKUP_DIR=/var/lib/bithumb-bot/backup/live/db
```

추가 권장:

```dotenv
REPORT_ROOT=/var/lib/bithumb-bot/data/live/reports
RAW_DATA_ROOT=/var/lib/bithumb-bot/data/live/raw
DERIVED_DATA_ROOT=/var/lib/bithumb-bot/data/live/derived
TRADES_DATA_ROOT=/var/lib/bithumb-bot/data/live/trades
```

---

## 6. 모드별 저장 규칙

### paper
- 모의 체결 전용
- 실거래 자격 없음
- live와 DB/lock/backups 공유 금지

### live
- 실제 주문/실제 체결
- 가장 엄격한 백업/로그/감사 기준 적용
- `DB_PATH`, notifier, risk limit, arming 조건 필수

### dryrun (향후 도입 시)
- 전략/주문 직전 판단은 실제처럼 실행
- 주문 전송만 차단
- live와 별도 분리

---

## 7. 저장 형식 권장안

### SQLite
용도:
- 내부 상태 복구
- 현재 포지션/주문/체결/헬스 원장

### JSONL append-only
용도:
- 주문 요청/응답 기록
- 체결 이벤트 기록
- 전략 판단 근거
- reconcile 결과
- snapshot 증거 로그

권장 이유:
- 부분 손상 복구가 쉬움
- 날짜별 파일 회전이 쉬움
- pandas/jq/Python 후처리가 쉬움

---

## 8. 파일명 규칙

### 날짜 단위 파일
```text
orders_YYYY-MM-DD.jsonl
fills_YYYY-MM-DD.jsonl
balance_snapshots_YYYY-MM-DD.jsonl
strategy_YYYY-MM-DD.log
error_YYYY-MM-DD.log
```

### 시각 단위 스냅샷
```text
ops_report_YYYY-MM-DDTHHMMSSKST.txt
runtime_snapshot_YYYYMMDD_HHMMSS.tar.gz
```

규칙:
- KST 기준 날짜 사용
- 파일명에 공백 금지
- 환경명은 상위 경로로 표현하고 파일명에 중복하지 않는다.

---

## 9. 백업/보관 기준

### 백업 우선순위
1. DB
2. env redacted copy
3. reconcile / audit / errors 관련 파일
4. 전략 검증 리포트
5. raw market cache

### 보관 예시
- DB snapshot: 7일 daily + 최근 30개 유지
- logs: 최근 30일 hot, 이후 archive 이동
- raw market: 재수집 가능하면 단기 보관
- live trades/fills/balances: 장기 보관 우선

---

## 10. 현재 레포 기준 적용 메모

현재 코드베이스에는 다음 중간 상태가 보인다.
- `DB_PATH` 기본값이 `data/bithumb_1m.sqlite`
- `RUN_LOCK_PATH` 기본값이 `data/locks/...`
- `BACKUP_DIR` 기본값이 `backups`
- `.gitignore`로 `data/`, `tmp/`, `*.sqlite`, `*.log` 등을 제외하고 있음
- systemd는 명시적 `BITHUMB_ENV_FILE` 주입 구조를 이미 사용 중임

따라서 다음 방향으로 전환한다.
- 레포 상대 기본 경로는 로컬 개발 전용 fallback으로만 유지
- AWS 운영에서는 절대경로 env를 필수로 사용
- 이후 PathManager/StorageManager 도입 시 이 문서의 구조를 표준으로 삼는다.

---

## 11. 금지사항

다음은 운영 패치에서 금지한다.
- `./data`, `./backups`, `./tmp`, `./logs`에 직접 쓰기
- repo 하위에 live DB 생성
- paper/live 공용 DB 사용
- 임시 디버그 파일을 레포 루트에 남기기
- raw/derived/trades 구분 없이 한 폴더에 혼합 저장
- 새 모듈이 자체적으로 경로 문자열을 조립하는 것

---

## 12. 패치 승인 체크리스트

새 패치가 저장을 추가하면 반드시 아래를 명시한다.
- 어떤 환경(`paper/live/dryrun`)에 저장되는가
- 어떤 분류(`run/raw/derived/trades/reports/logs/backup`)에 속하는가
- 파일인가 DB인가
- append-only인가 overwrite인가
- 백업 대상인가
- 민감정보가 포함되는가
- retention 규칙이 필요한가

이 체크리스트를 통과하지 못한 저장 패치는 병합하지 않는다.
