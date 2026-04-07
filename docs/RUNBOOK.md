# RUNBOOK (Bithumb BTC 소액 실운영: 제한적 무인 운용)

> 범위 고정: **Bithumb / BTC 마켓만 / 현재 구현된 단일 전략만 / 제한적 무인 운용(주간 수시 점검 전제)**.
> 
> 이 문서는 **완전 24/7 자율운용** 가이드가 아니다. 운영자가 하루 중 여러 차례 상태를 확인하는 운용 모델을 기준으로 한다.

## 운영 모델 한 줄 요약 (pilot reality)

- 이 봇은 현재 **"제한적 무인 + 보수적 HALT"** 모델이다.
- `systemd` 재시작은 자동이지만, 위험/미해결 상태가 감지되면 **자동 재개 대신 차단(HALT/Resume gate)** 이 우선된다.
- 즉, "항상 자동 복구되는 24/7 자율 시스템"이 아니라 **운영자 재정합(reconcile) + 승인 재개(resume)** 를 전제로 한 pilot 단계다.
- 거래소 응답 지연/누락 체결 같은 경계 사례는 환경마다 다를 수 있으므로, **live에서는 `recovery-report` 결과를 최종 판단 기준**으로 사용한다.

## 0) 운용 모드 구분 (반드시 먼저 확인)

아래 4가지를 혼동하지 않는다.

- [ ] **paper**: 시뮬레이션 운용. 실거래소 자금/주문 영향 없음.
- [ ] **live + dry-run**: 라이브 경로 점검 모드. 거래소 조회는 가능하지만 실주문은 금지 (`LIVE_DRY_RUN=true`).
- [ ] **live + armed**: 실주문 허용 모드 (`LIVE_DRY_RUN=false`, `LIVE_REAL_ORDER_ARMED=true`).
- [ ] **live + not-armed**: `LIVE_DRY_RUN=false`라도 `LIVE_REAL_ORDER_ARMED!=true`면 fail-fast로 기동 실패해야 정상.

운영 시작 전 선언:

- [ ] 오늘 세션 목적은 `paper` / `live dry-run` / `live armed` 중 하나로 명확히 선택했다.
- [ ] BTC 외 심볼/다중자산 운용은 하지 않는다.
- [ ] 단일 전략 외 커스텀 실험 코드는 라이브에 올리지 않는다.

## 1) 1,000,000 KRW 소액 계정 보수 프로필 (권장)

실거래 초기값은 아래처럼 보수적으로 시작한다.

- `MAX_ORDER_KRW=30000` (계정의 약 3%)
- `MAX_DAILY_LOSS_KRW=20000` (계정의 약 2% 손실 시 즉시 HALT(무기한 중지, 자동 재개 없음))
- `MAX_DAILY_ORDER_COUNT=6` (과매매/오작동 노출 축소)
- `KILL_SWITCH=false`, `KILL_SWITCH_LIQUIDATE=false` (평시 off; 필요 시에만 비상 정지/청산 절차에 따라 사용)
- `LIVE_DRY_RUN=true`로 먼저 운영 경로를 검증하고, 확인 후 `false` 전환
- 일 손실 한도 초과 시 엔진은 신규 주문 전 단계에서 거래를 **HALT**하고 오픈주문 취소 + 포지션 평탄화(flatten)를 시도한 뒤, 노출/미해결 상태가 남으면 운영자 복구/재개 승인을 요구한다.

> 핵심 원칙: **주문 크기보다 생존이 우선**. 초반 1~2주는 수익보다 안정성 검증에 집중.

## 2) 배포 구성

- `deploy/systemd/bithumb-bot.service`: 메인 트레이딩 루프 (`Restart=always`).
- `deploy/systemd/bithumb-bot-healthcheck.timer`: 1분마다 상태 점검.
- `deploy/systemd/bithumb-bot-backup.timer`: 6시간마다 SQLite 백업.
- `scripts/healthcheck.py`: stale candle / 오류 횟수 / trading disabled 감지.
- `scripts/backup_sqlite.sh`: sqlite `.backup` 기반 스냅샷 + 보관 정책.

플랫폼 범위:

- 운영 대상: Linux (예: Ubuntu, AWS EC2 Linux)
- native Windows는 `run` lock(`fcntl`) 미지원으로 운영 대상 아님
- Windows 사용자는 WSL2(Linux)에서 실행

## 3) 설치 및 활성화

```bash
sudo mkdir -p /etc/bithumb-bot
sudo cp .env.example /etc/bithumb-bot/bithumb-bot.live.env

RENDER_DIR="$(mktemp -d)"
BITHUMB_BOT_ROOT="$(pwd)" \
BITHUMB_UV_BIN="$(command -v uv)" \
BITHUMB_RUN_USER="$(id -un)" \
./deploy/systemd/render_units.sh "${RENDER_DIR}"
sudo cp "${RENDER_DIR}"/bithumb-bot.service /etc/systemd/system/
sudo cp "${RENDER_DIR}"/bithumb-bot-healthcheck.service /etc/systemd/system/
sudo cp "${RENDER_DIR}"/bithumb-bot-healthcheck.timer /etc/systemd/system/
sudo cp "${RENDER_DIR}"/bithumb-bot-backup.service /etc/systemd/system/
sudo cp "${RENDER_DIR}"/bithumb-bot-backup.timer /etc/systemd/system/

sudo systemctl daemon-reload
sudo systemctl enable --now bithumb-bot.service
sudo systemctl enable --now bithumb-bot-healthcheck.timer
sudo systemctl enable --now bithumb-bot-backup.timer
```

- 운영 전 반드시 3개 유닛의 env/DB 일관성을 점검한다.
  - `bithumb-bot.service`: `Environment=BITHUMB_ENV_FILE=/etc/bithumb-bot/bithumb-bot.live.env`
  - `bithumb-bot-healthcheck.service`, `bithumb-bot-backup.service`: `Environment=BITHUMB_ENV_FILE=/etc/bithumb-bot/bithumb-bot.live.env`
  - 세 유닛이 같은 env 파일을 보므로 `DB_PATH`, notifier, 임계치를 단일 파일 기준으로 관리.
- `bithumb-bot.service` / `bithumb-bot-paper.service`는 `PYTHONUNBUFFERED=1` + `python -u` + `@BITHUMB_UV_BIN@` 경로를 사용해 systemd/journald 환경에서도 런루프 로그가 즉시 출력되도록 유지한다.
- `bithumb-bot-healthcheck.service`는 `User=@BITHUMB_RUN_USER@`, `WorkingDirectory=@BITHUMB_BOT_ROOT@`, `ExecStart=@BITHUMB_UV_BIN@ run python @BITHUMB_BOT_ROOT@/scripts/healthcheck.py` 템플릿으로 렌더링한다.
- healthcheck는 fail-fast 정책이다.
  - `BITHUMB_ENV_FILE`이 비어 있거나 파일이 없으면 즉시 실패
  - env 파일 내 `DB_PATH`가 비어 있어도 실패(기본 DB 자동 대체 금지)

## 4) 프리라이브(안전진입) 체크리스트

아래는 **실주문 진입 전**에만 수행하는 체크리스트다. 하나라도 실패하면 live armed로 넘어가지 않는다.

### A. 환경/권한/리스크 설정 확인

1. `MODE`가 의도한 모드인지 확인 (`paper`/`live` 혼동 금지)
2. `MODE=live`라면 다음 값이 의도대로 설정되었는지 재확인
   - `MAX_ORDER_KRW > 0`
   - `MAX_DAILY_LOSS_KRW > 0`
   - `MAX_DAILY_ORDER_COUNT > 0`
   - `MAX_ORDERBOOK_SPREAD_BPS > 0` (유한값)
   - `MAX_MARKET_SLIPPAGE_BPS > 0` (유한값)
   - `LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS > 0` (유한값)
3. `MODE=live` 기본 진입은 `LIVE_DRY_RUN=true`로 시작
4. 실주문 전환 직전에만 `LIVE_DRY_RUN=false` + `LIVE_REAL_ORDER_ARMED=true` 동시 설정
5. `KILL_SWITCH=false` 확인 (비상 시에만 true)
6. `KILL_SWITCH_LIQUIDATE`는 필요 시 비상 flatten 시도용으로만 사용
7. `.env.example` 복사본을 그대로 쓰지 말고 live 필수값을 명시적으로 덮어쓴다
   - 기본/공유 DB 경로(`data/bithumb_1m.sqlite`) 금지
   - paper 전용 키(`START_CASH_KRW`, `BUY_FRACTION`, `FEE_RATE`, `PAPER_FEE_RATE`, `PAPER_FEE_RATE_ESTIMATE`, `SLIPPAGE_BPS`)는 live에서 unset
8. API 키 권한 확인 (수동 점검)
   - 조회 + 주문(현물) 권한이 있는지 확인
   - 출금 권한은 비활성화 권장
   - API 키는 env 파일에만 저장하고 직전 주입 원칙 유지
   - IP whitelist(사용 시) 포함 권한 스코프는 코드가 자동 검증하지 않으므로 운영자가 직접 확인
9. DB 분리 확인
   - `paper`와 `live`는 서로 다른 `DB_PATH` 사용
   - `MODE=live`에서 기본 DB 경로 사용 금지 규칙 준수

### B. 프리라이브 명령 순서 (고정)

아래 순서를 **그대로** 실행한다.

```bash
uv run bithumb-bot broker-diagnose
uv run bithumb-bot health
uv run bithumb-bot recovery-report
uv run bithumb-bot reconcile
uv run bithumb-bot recovery-report
```

판정:

- `broker-diagnose`가 `overall=PASS`가 아니면 실주문 금지 (`overall=WARN/FAIL` 모두 보류)
- `health`에서 stale/error 이상이 있으면 원인 해소 전 진행 금지
- `recovery-report`에서 unresolved/recovery-required가 남아 있으면 `reconcile` 후 재확인

### C. 서비스 기동/로그 확인

```bash
sudo systemctl restart bithumb-bot.service
sudo systemctl status bithumb-bot.service
sudo journalctl -u bithumb-bot.service -n 100 --no-pager

uv run bithumb-bot health
uv run bithumb-bot recovery-report
```

- 서비스가 `active (running)`인지 확인.
- `health`에서 `last_candle_age_sec`, `error_count`, `trading_enabled` 확인.
- `recovery-report`에서 unresolved/recovery-required 건수와 오래된 미해결 주문 요약(top 5) 확인.

## 5) 기본 점검 (일상 운용)

```bash
sudo systemctl list-timers | rg 'bithumb-bot-(healthcheck|backup)'
./scripts/backup_sqlite.sh
```

- timer가 정상 등록/실행되는지 확인.
- `backups/` 파일 생성 여부 확인.

## 5-1) 브로커 읽기 전용 진단 (`broker-diagnose`)

실주문 전/장애 조사 시 **주문 없이** 거래소 연동 상태를 빠르게 점검한다.

```bash
uv run bithumb-bot broker-diagnose
```

출력 요약 항목:

- 헤더: `[BROKER-READINESS]`, `pair=<PAIR>`
- 요약: `summary: pass=<N> warn=<N> fail=<N> overall=PASS|WARN|FAIL`
- 상세: `- [PASS|WARN|FAIL] <check name>: <detail>`

운영 가이드:

- `overall=PASS`: 라이브 전 점검 통과(다음 단계 진행 가능)
- `overall=WARN`: 비치명 경고 존재(원인 확인 후 진행 여부 판단)
- `overall=FAIL`: 핵심 조회 실패(비정상). 원인 해소 전 재개/실주문 금지

주의:

- `MODE=live`에서만 동작한다. 그 외 모드에서는 실패로 종료한다.
- 이 명령은 주문 생성/취소를 호출하지 않는 읽기 전용 진단이다.

## 6) 운영자 즉시 제어 체크리스트 (pause/resume/cancel)

문제 징후(오류 급증, 체결/잔고 불일치 의심, 네트워크 불안정) 시 아래 3개 명령을 우선 사용한다.

### A. 즉시 일시중지

```bash
uv run bithumb-bot pause
```

- 신규 주문 차단이 최우선.
- pause 직후 `health` + `recovery-report` + 최근 로그를 본다.

### B. 오픈 주문 정리

```bash
uv run bithumb-bot cancel-open-orders
```

- live 모드 원격 미체결 주문을 정리한다.
- 실행 후 `reconcile` + `recovery-report`로 정합성 재확인.

### C. 재개

```bash
uv run bithumb-bot resume
```

- blocker가 남아 있으면 재개하지 않는다.
- `resume --force`는 마지막 수단으로만 사용한다.

## 7) 비상 정지 / 일시중지 / 복구 체크리스트

### A. 즉시 리스크 차단 (Emergency stop)

```bash
# 1) 신규 거래 즉시 중지
uv run bithumb-bot pause

# 2) (선택) 환경에서 kill switch 활성화 후 서비스 재시작
# KILL_SWITCH=true
# sudo systemctl restart bithumb-bot.service
```

- 원인 확인 전에는 `resume --force`를 사용하지 않는다.
- 운영자 승인 없이 실주문 재개 금지.

### B. 상태 파악 (Pause 상태에서)

```bash
uv run bithumb-bot health
uv run bithumb-bot recovery-report
sudo journalctl -u bithumb-bot.service -n 200 --no-pager
```

확인 포인트:
- 최근 오류가 API/네트워크/인증 중 무엇인지
- 미해결 주문(`unresolved_orders`) 존재 여부
- 복구 필요 주문(`recovery_required_orders`) 존재 여부
- 오래된 주문 요약에서 `client_order_id`, `exchange_order_id`, `last_error`를 우선 확인해 복구 우선순위 결정

### C. 복구 액션 (필요 시 순서대로)

```bash
# live 모드에서 원격 미체결 일괄 취소
uv run bithumb-bot cancel-open-orders

# 거래소/로컬 원장 정합성 점검
uv run bithumb-bot reconcile

# 복구 상태 재확인
uv run bithumb-bot recovery-report
```

### D. 재개 (Recovery / Resume)

```bash
# 보수적 재개 (이상 상태가 있으면 자동 거부)
uv run bithumb-bot resume

# 마지막 수단: 운영자 책임 하 강제 재개
uv run bithumb-bot resume --force
```

리스크 사유(`KILL_SWITCH`, `DAILY_LOSS_LIMIT`, `POSITION_LOSS_LIMIT`)로 HALT된 경우 추가 규칙:

- 포지션/오픈오더 등 노출(exposure)이 남아 있으면 `resume`은 거부된다.
- 엔진은 오픈주문 취소와 flatten을 시도하지만, 실패/미해결 시 운영자가 먼저 노출을 수동 해소해야 한다.
- 해소 후 `recovery-report`와 `health`를 다시 확인하고 `resume`을 실행한다.

`recover-order`는 `RECOVERY_REQUIRED` 상태 주문에만 적용되며, 완료 후에도 거래는 자동 재개되지 않는다.


예시 (`uv run bithumb-bot recovery-report`):

```text
[P2] resume_eligibility
  resume_allowed=0
  can_resume=false
  blockers=STARTUP_SAFETY_GATE_BLOCKED, HALT_RISK_OPEN_POSITION
  force_resume_allowed=0
```

## 8) tiny-size 실주문 스모크 테스트 (armed-live 직후 1회)

실주문 전환 직후에는 아래를 1회 수행해 주문-체결-기록 루프를 소액으로 검증한다.

1. 주문 한도를 일시적으로 가장 작은 안전 값으로 유지 (`MAX_ORDER_KRW` 최소)
2. `MODE=live`, `LIVE_DRY_RUN=false`, `LIVE_REAL_ORDER_ARMED=true` 확인
3. 1회 주문/체결 발생을 모니터링 (주문 과다 유도 금지)
4. 즉시 다음 확인
   - `uv run bithumb-bot health`
   - `uv run bithumb-bot recovery-report`
   - `uv run bithumb-bot reconcile`
5. `orders/fills/trades`에 1회 사이클이 정상 기록되면 스모크 통과
6. 이상 징후 시 `pause` -> `cancel-open-orders` -> 원인 분석 후 재진입

## 9) 크래시/재시작 후 재정합 검증 (restart-and-reconcile)

크래시/강제 재시작 이후에는 아래를 모두 확인하기 전 재개하지 않는다.

1. `journalctl`로 마지막 예외 원인이 해소되었는지 확인
2. `uv run bithumb-bot recovery-report`에서 아래가 0인지 확인
   - `unresolved_orders`
   - `recovery_required_orders`
3. `uv run bithumb-bot reconcile` 후 다시 `recovery-report` 실행
4. live 모드면 거래소 오픈 주문/체결과 로컬 `orders/fills/trades` 샘플 대조
5. `uv run bithumb-bot health`에서 stale/error 이상 없음 확인
6. `uv run bithumb-bot resume`로 재개 후 30~60분 모니터링

### 재시작/복구 표준 플로우 (운영자용 고정 절차)

아래 순서를 기본값으로 사용한다.

```bash
# 0) (필요 시) 즉시 신규 주문 차단
uv run bithumb-bot pause

# 1) 상태 확인
uv run bithumb-bot health
uv run bithumb-bot recovery-report

# 2) 정합성 복구
uv run bithumb-bot reconcile
uv run bithumb-bot recovery-report

# 3) 미체결 노출이 남으면 정리 후 재검증
uv run bithumb-bot cancel-open-orders
uv run bithumb-bot reconcile
uv run bithumb-bot recovery-report

# 4) blocker 해소 시에만 재개
uv run bithumb-bot resume
```

운영 규칙:

- `resume`이 거부되면 먼저 blocker를 해소한다 (`resume --force` 상시 사용 금지).
- 재개 판정은 단순 `unresolved_count`가 아니라 `resume_blockers`(예: `STARTUP_SAFETY_GATE_BLOCKED`, `LAST_RECONCILE_FAILED`, `HALT_RISK_OPEN_POSITION`) 기반이다.
- 리스크 사유 HALT(`KILL_SWITCH`, `DAILY_LOSS_LIMIT`, `POSITION_LOSS_LIMIT`)에서는 노출(포지션/오픈오더) 정리 전 재개가 제한될 수 있다.
- 강제 재개(`resume --force`)는 조사/승인 로그를 남긴 경우에만 예외적으로 사용한다.

## 10) 장애 대응 절차 (유형별)

### A. 재시작/프로세스 크래시

1. `sudo systemctl status bithumb-bot.service`로 재시작 루프 여부 확인.
2. `sudo journalctl -u bithumb-bot.service -n 200 --no-pager`로 직전 예외 확인.
3. 환경변수/설정 수정 후 `sudo systemctl restart bithumb-bot.service`.
4. 3~5분 모니터링 후 healthcheck 알림 미발생 확인.

### B. 중복 주문 의심

1. `uv run bithumb-bot orders --limit 100`로 최근 주문 상태 확인.
2. 동일 시점/동일 방향의 order가 중복인지 확인.
3. live 모드면 거래소 체결 내역과 `fills` 비교.
4. 필요시 봇 일시 중지: `uv run bithumb-bot pause`.
5. 수동 정리 후 재기동/재개.

### C. 잔고 불일치

1. `uv run bithumb-bot audit` 실행.
2. 불일치 시 `uv run bithumb-bot pnl --days 1` 및 `trades`/`fills` 대조.
3. live 모드면 브로커 잔고 API 기준으로 reconcile 수행.
4. 원인 확인 전 신규 주문 중단.

### D. 데이터 누락 (캔들 stale / sync 실패)

1. `uv run bithumb-bot health` 확인 (`last_candle_age_sec`).
2. `journalctl`에서 `sync failed`, `stale candle` 로그 확인.
3. 네트워크/API 상태 확인 후 `sudo systemctl restart bithumb-bot.service`.
4. 재발 시 `EVERY`, `INTERVAL`, rate limit 설정 완화.

### E. 레이트 리밋 대응

1. 에러 로그에서 HTTP 429/거래소 에러코드 확인.
2. `EVERY` 증가, 재시도/호출 빈도 완화.
3. healthcheck 알림 빈도가 높으면 임계치(`HEALTH_MAX_ERROR_COUNT`) 조정.
4. 복구 후 10~15분간 주문/체결/캔들 흐름 점검.

## 11) 알림 설정

하나 이상 설정하면 webhook 알림 사용, 미설정 시 콘솔 출력만 수행.

- Generic webhook: `NOTIFIER_WEBHOOK_URL`
- Slack incoming webhook: `SLACK_WEBHOOK_URL`
- Telegram bot: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`

권장:

- `NOTIFIER_ENABLED=true`
- `NOTIFIER_TIMEOUT_SEC=5`
- 비밀키/URL은 env 파일에만 저장하고 로그에 출력 금지.

## 12) 백업 정책

- 기본 경로: `/var/lib/bithumb-bot/backup/<mode>/db/`
- 기본 보관: 7일, 최대 30개
- 환경변수:
  - `BACKUP_DIR`
  - `BACKUP_RETENTION_DAYS`
  - `BACKUP_RETENTION_COUNT`
  - `BACKUP_VERIFY_RESTORE=1` (백업 직후 `tools/verify_sqlite_restore.py`로 복구 읽기 검증)

복구 예시:

```bash
sqlite3 /var/lib/bithumb-bot/data/live/trades/live.sqlite ".restore /var/lib/bithumb-bot/backup/live/db/live.sqlite.20260101_120000.sqlite"

# 백업 파일 복구 검증(권장)
python3 tools/verify_sqlite_restore.py /var/lib/bithumb-bot/backup/live/db/live.sqlite.20260101_120000.sqlite
```

## 13) Live 모드 사전 점검 (fail-fast)

`MODE=live`로 시작하면 런타임 시작 전에 아래 항목을 강제 검증한다. 하나라도 누락되면 즉시 종료된다.

- `MAX_ORDER_KRW > 0`
- `MAX_DAILY_LOSS_KRW > 0`
- `MAX_DAILY_ORDER_COUNT > 0`
- `DB_PATH`는 `MODE=live`에서 반드시 명시해야 하며, 상대경로 사용 금지(절대경로 필수)
- live preflight는 paper/test 성격 혼합 설정을 차단한다(예: `START_CASH_KRW`, `BUY_FRACTION`, `FEE_RATE`, `PAPER_FEE_RATE`, `PAPER_FEE_RATE_ESTIMATE`, `SLIPPAGE_BPS`가 설정된 경우 거부)
- `MAX_ORDERBOOK_SPREAD_BPS`, `MAX_MARKET_SLIPPAGE_BPS`, `LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS`는 live에서 `>0` 유한값 필수
- `LIVE_DRY_RUN=false`인 경우 `BITHUMB_API_KEY`, `BITHUMB_API_SECRET` 필수
- `LIVE_DRY_RUN=false`인 경우 `LIVE_REAL_ORDER_ARMED=true`를 명시해야 실주문 허용
- `/v1/accounts` preflight에서 quote 통화 row(예: KRW)는 항상 필수이며, `LIVE_DRY_RUN=true` + `LIVE_REAL_ORDER_ARMED=false` 조합에서는 base 통화 row 누락을 0 보유(무포지션 시작)로 해석해 통과 가능
- 실주문 경로(`LIVE_DRY_RUN=false` + `LIVE_REAL_ORDER_ARMED=true`)에서는 base 통화 row 누락을 허용하지 않으며, 즉시 fail-fast 차단된다.
- 운영자 진단(`broker-diagnose`, `health`, `ops-report`)에서는 `/v1/accounts` 관련 상태를 `execution_mode`, `quote_currency`, `base_currency`, `base_currency_missing_policy`, `preflight_outcome` 필드로 함께 출력한다.  
  - 예: `preflight_outcome=pass_no_position_allowed`(dry-run 무포지션 허용 통과), `preflight_outcome=fail_real_order_blocked`(실주문 경로 차단)
- notifier는 반드시 활성/설정되어야 함(`NOTIFIER_WEBHOOK_URL` 또는 `SLACK_WEBHOOK_URL` 또는 `TELEGRAM_BOT_TOKEN`+`TELEGRAM_CHAT_ID`)
- `KILL_SWITCH_LIQUIDATE`는 live preflight 실패 사유가 아니며, kill switch 동작 시 flatten 시도 여부를 제어한다

실주문 전환 절차(arming):

1. `LIVE_DRY_RUN=true` 상태로 로그/알림/복구 동작을 먼저 검증
2. 실주문 시작 직전에 `LIVE_DRY_RUN=false` + `LIVE_REAL_ORDER_ARMED=true`를 함께 설정
3. `LIVE_REAL_ORDER_ARMED=true`가 없으면 live preflight에서 즉시 종료(fail-fast)


Live 보수 preset 예시(소액 계정 + 알림 포함):

```bash
# 1) paper에서 먼저 검증 (DB 분리)
MODE=paper DB_PATH=/var/lib/bithumb-bot/data/paper/trades/paper.small.safe.sqlite \
NOTIFIER_ENABLED=true SLACK_WEBHOOK_URL=https://hooks.slack.com/services/xxx/yyy/zzz \
MAX_ORDER_KRW=30000 MAX_DAILY_LOSS_KRW=20000 MAX_DAILY_ORDER_COUNT=6 \
uv run bithumb-bot run

# 2) live dry-run (실주문 API 미호출)
MODE=live DB_PATH=/var/lib/bithumb-bot/data/live/trades/live.small.safe.sqlite LIVE_DRY_RUN=true LIVE_REAL_ORDER_ARMED=false \
NOTIFIER_ENABLED=true SLACK_WEBHOOK_URL=https://hooks.slack.com/services/xxx/yyy/zzz \
MAX_ORDER_KRW=30000 MAX_DAILY_LOSS_KRW=20000 MAX_DAILY_ORDER_COUNT=6 \
uv run bithumb-bot run

# 3) 실주문 arming (운영자 명시 승인 후 직전에만)
MODE=live DB_PATH=/var/lib/bithumb-bot/data/live/trades/live.small.safe.sqlite LIVE_DRY_RUN=false LIVE_REAL_ORDER_ARMED=true \
NOTIFIER_ENABLED=true SLACK_WEBHOOK_URL=https://hooks.slack.com/services/xxx/yyy/zzz \
MAX_ORDER_KRW=30000 MAX_DAILY_LOSS_KRW=20000 MAX_DAILY_ORDER_COUNT=6 \
BITHUMB_API_KEY=... BITHUMB_API_SECRET=... \
uv run bithumb-bot run
```

## Health / Recovery Reading

- Runtime artifacts such as health/recovery reports and snapshots belong under env-injected runtime roots, not repo-relative paths like `./data`, `./backups`, or `./tmp`.
- `health` is a live status snapshot, not a green/red verdict. Read `trading_enabled`, `halt_new_orders_blocked`, `unresolved_open_order_count`, and `recovery_required_count` together before deciding to resume.
- `effective_flat_due_to_harmless_dust=1` can still mean a real BTC remainder exists. Treat it as an operator interpretation of a small remainder, not a literal zero-balance claim.
- `dust_state=matched_harmless_dust` means broker/local dust is close enough to be harmless under policy. It is only resume-safe when the matching policy also allows resume.
- `dust_state=dangerous_dust` means the remainder is not safely resumable yet. Keep new orders blocked until the broker, DB, and recovery evidence all line up.
- `accounts_flat_start_allowed=True` is only a `/v1/accounts` diagnostic. It does not override `recovery-report` blockers.
- `order_rules_autosync=FALLBACK` means `/v1/orders/chance` rule data was not available and the bot is using local fallback constraints. In live mode, clear that warning before real-order arming.
- `/v2/orders` pre-validation still applies after the rule snapshot is loaded: market buys are `side=bid, order_type=price, price=<KRW>`, and market sells are `side=ask, order_type=market, volume=<qty>`.

## Dust Residual Operational Reading

- `dust residual` means the remaining BTC is small enough that one or both exchange sell gates may fail: minimum quantity and minimum notional. "Dust" is about sellability, not about whether order recovery is complete.
- `matched dust` means broker/app balance and local DB balance match closely enough, and the remaining position is classified as `matched_harmless_dust`. That label can still mean a real BTC remainder exists; it is an operator reading that the remainder is harmless enough to resume only when policy also allows it.
- `dangerous dust` means the remainder is classified as `dangerous_dust`: broker/local quantities do not match safely, or the quantity is small but still operationally risky.
- `unresolved order` means order lifecycle consistency is still unclear. This is not the same as dust and must be treated as a higher-risk condition.
- Before restart, check in this order:
  1. `uv run bithumb-bot health`
  2. `uv run bithumb-bot recovery-report`
  3. `uv run bithumb-bot ops-report --limit 20`
- Read the outputs in this order:
  1. `recovery-report [P1]` and `[P2]` decide whether restart is allowed.
  2. `recovery-report [P3.0]` explains whether any remaining position is dust-only or restart-blocking dust.
  3. `ops-report` is the operator cross-check for dust numbers and `/v1/accounts` diagnostics.
- `resume_allowed=0` and `can_resume=false` always mean do not restart yet. If the blocker list includes `MATCHED_DUST_POLICY_REVIEW_REQUIRED` or `DANGEROUS_DUST_REVIEW_REQUIRED`, treat that as a dust policy gate, not as proof of unresolved orders.
- `accounts_flat_start_allowed=True` is only a `/v1/accounts` preflight diagnostic. It never overrides `recovery-report` restart blockers.
- If `dust_state=dangerous_dust`, treat the bot as restart-blocked for new orders even when `/v1/accounts` diagnostics say `accounts_flat_start_allowed=True`.
- If `dust_state=matched_harmless_dust`, the remainder may be operationally flat only when all of these are true:
  1. `recovery-report` shows `unresolved_count=0`
  2. `recovery-report` shows `recovery_required_count=0`
  3. `recovery-report [P3.0]` shows `allow_resume=1` and `resume_allowed_by_policy=1`
- If `unresolved_count > 0` or `recovery_required_count > 0`, do not downgrade the situation to dust-only until recovery evidence is clear.
- For manual review, compare three views before any resume decision:
  1. app view: `health` / `recovery-report` / `ops-report` dust fields
  2. DB view: local position and recent sell evidence represented by `dust_local_qty`, `recent_dust_unsellable_event`, unresolved counts, and recovery-required counts
  3. broker view: `/v1/accounts` diagnostics and broker quantity represented by `dust_broker_qty`
- `dust_broker_qty` and `dust_local_qty` should be read together with `dust_broker_local_match`. A small remainder is only resume-safe when the broker/local remainder matches closely enough and the policy marks it resume-safe.
- `dust_min_qty` and `dust_min_notional_krw` are different gates. A sell can be blocked because quantity is below minimum, because notional is below minimum, or because both are below minimum. Do not assume one implies the other, and do not assume a remainder above one minimum is tradable before checking the other.
- `effective_flat_due_to_harmless_dust=1` is a reporting convenience, not proof of a literal zero balance. If the bot is in this state, keep reading the broker/DB quantities before deciding whether the position is actually flat.

## Manual App Sell Caution

- Operating direction: do not rely on manual app sells as the normal dust-handling path. The preferred path is `reconcile` plus report review, then resume only if policy allows it.
- If the bot is stopped and you manually sell in the exchange app, run `health`, `recovery-report`, and `ops-report` again before restarting.
- Manual app sells can leave dust smaller than exchange minimums. In that case, another sell attempt may fail or create misleading operator signals.
- Before any manual sell retry, confirm all of the following:
  1. `unresolved_count=0` and `recovery_required_count=0`
  2. broker/local dust numbers are understood from `dust_broker_qty`, `dust_local_qty`, and `dust_broker_local_match`
  3. both exchange limits are checked: `dust_min_qty` and `dust_min_notional_krw`
  4. the intended sell size is actually above both minimums after quantity-step and decimal normalization
- Do not assume "almost zero balance" means restart is safe. Confirm `dust_state`, `dust_action`, `dust_resume_allowed_by_policy`, and unresolved order counts first.
- If `dust_state=dangerous_dust`, resume is not allowed. Reconcile, compare app/DB/broker state, and keep new orders blocked until the residual is explained.
- If `dust_state=matched_harmless_dust` but `dust_resume_allowed_by_policy=0`, treat it as a review-required matched dust case: exposure can be treated as flat for interpretation, but restart and new orders still stay blocked.
- Do not use `resume --force` as a shortcut around dust review. First confirm this is dust only and not an unresolved order or mismatched broker/local state.
- Prefer `reconcile` plus report review over `resume --force` whenever broker balance changed outside the bot.
