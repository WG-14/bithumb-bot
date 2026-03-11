# RUNBOOK (24/7 운영 초안)

## 0) 1,000,000 KRW 소액 계정 보수 프로필 (권장)

실거래 초기값은 아래처럼 보수적으로 시작한다.

- `MAX_ORDER_KRW=30000` (계정의 약 3%)
- `MAX_DAILY_LOSS_KRW=20000` (계정의 약 2% 손실 시 즉시 HALT(무기한 중지, 자동 재개 없음))
- `MAX_DAILY_ORDER_COUNT=6` (과매매/오작동 노출 축소)
- `KILL_SWITCH=false`, `KILL_SWITCH_LIQUIDATE=false` (평시 off, **청산 모드 미구현으로 true 금지**)
- `LIVE_DRY_RUN=true`로 먼저 운영 경로를 검증하고, 확인 후 `false` 전환
- 일 손실 한도 초과 시 엔진은 신규 주문 전 단계에서 거래를 **영구 HALT**하고 오픈주문 취소만 1회 시도한다(자동 재개/강제 청산 없음).

> 핵심 원칙: **주문 크기보다 생존이 우선**. 초반 1~2주는 수익보다 안정성 검증에 집중.

## 1) 배포 구성

- `deploy/systemd/bithumb-bot.service`: 메인 트레이딩 루프 (`Restart=always`).
- `deploy/systemd/bithumb-bot-healthcheck.timer`: 1분마다 상태 점검.
- `deploy/systemd/bithumb-bot-backup.timer`: 6시간마다 SQLite 백업.
- `scripts/healthcheck.py`: stale candle / 오류 횟수 / trading disabled 감지.
- `scripts/backup_sqlite.sh`: sqlite `.backup` 기반 스냅샷 + 보관 정책.

## 2) 설치 및 활성화

```bash
sudo mkdir -p /etc/bithumb-bot
sudo cp .env.example /etc/bithumb-bot/bithumb-bot.env

sudo cp deploy/systemd/bithumb-bot.service /etc/systemd/system/
sudo cp deploy/systemd/bithumb-bot-healthcheck.service /etc/systemd/system/
sudo cp deploy/systemd/bithumb-bot-healthcheck.timer /etc/systemd/system/
sudo cp deploy/systemd/bithumb-bot-backup.service /etc/systemd/system/
sudo cp deploy/systemd/bithumb-bot-backup.timer /etc/systemd/system/

sudo systemctl daemon-reload
sudo systemctl enable --now bithumb-bot.service
sudo systemctl enable --now bithumb-bot-healthcheck.timer
sudo systemctl enable --now bithumb-bot-backup.timer
```

## 3) 라이브 시작 체크리스트 (Startup)

### A. 환경/리스크 값 확인

1. `MODE=live` 여부 확인 (paper/live 혼동 금지)
2. 다음 값이 의도대로 설정되었는지 재확인
   - `MAX_ORDER_KRW=30000`
   - `MAX_DAILY_LOSS_KRW=20000`
   - `MAX_DAILY_ORDER_COUNT=6`
3. 실주문 전에는 `LIVE_DRY_RUN=true`
4. `KILL_SWITCH=false` 확인 (비상 시에만 true)
5. `KILL_SWITCH_LIQUIDATE=false` 확인 (청산 모드 미구현; true면 기동 실패)
6. API 키는 `LIVE_DRY_RUN=false` 전환 직전에만 주입

### B. 기동 전/직후 점검

```bash
sudo systemctl restart bithumb-bot.service
sudo systemctl status bithumb-bot.service
sudo journalctl -u bithumb-bot.service -n 100 --no-pager

uv run python bot.py health
uv run python bot.py recovery-report
```

- 서비스가 `active (running)`인지 확인.
- `health`에서 `last_candle_age_sec`, `error_count`, `trading_enabled` 확인.
- `recovery-report`에서 unresolved/recovery-required 건수와 오래된 미해결 주문 요약(top 5) 확인.

## 4) 기본 점검

```bash
sudo systemctl list-timers | rg 'bithumb-bot-(healthcheck|backup)'
./scripts/backup_sqlite.sh
```

- timer가 정상 등록/실행되는지 확인.
- `backups/` 파일 생성 여부 확인.

## 4-1) 브로커 읽기 전용 진단 (`broker-diagnose`)

실주문 전/장애 조사 시 **주문 없이** 거래소 연동 상태를 빠르게 점검한다.

```bash
uv run python bot.py broker-diagnose
```

출력 요약 항목:

- `connectivity`: 브로커/API 기본 연결 및 잔고 조회 성공 여부
- `balances`: 가용/잠금 현금·자산
- `market_rules`: 최소 수량/스텝/최소 주문금액/소수점 자릿수
- `open_orders`: 원격 미체결 주문 개수
- `recent_orders`: 최근 주문 조회 지원 여부 및 상태별 요약
- `overall_status`: `OK` / `PARTIAL` / `FAILED`

운영 가이드:

- `OK`: 라이브 전 점검 통과(다음 단계 진행 가능)
- `PARTIAL`: 일부 조회 실패(네트워크/API 상태 확인 후 재시도 권장)
- `FAILED`: 핵심 조회 실패(비정상). 원인 해소 전 재개/실주문 금지

주의:

- `MODE=live`에서만 동작한다. 그 외 모드에서는 실패로 종료한다.
- 이 명령은 주문 생성/취소를 호출하지 않는 읽기 전용 진단이다.

## 5) 비상 정지 / 일시중지 / 복구 체크리스트

### A. 즉시 리스크 차단 (Emergency stop)

```bash
# 1) 신규 거래 즉시 중지
uv run python bot.py pause

# 2) (선택) 환경에서 kill switch 활성화 후 서비스 재시작
# KILL_SWITCH=true
# sudo systemctl restart bithumb-bot.service
```

- 원인 확인 전에는 `resume --force`를 사용하지 않는다.
- 운영자 승인 없이 실주문 재개 금지.

### B. 상태 파악 (Pause 상태에서)

```bash
uv run python bot.py health
uv run python bot.py recovery-report
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
uv run python bot.py cancel-open-orders

# 거래소/로컬 원장 정합성 점검
uv run python bot.py reconcile

# 복구 상태 재확인
uv run python bot.py recovery-report
```

### D. 재개 (Recovery / Resume)

```bash
# 보수적 재개 (이상 상태가 있으면 자동 거부)
uv run python bot.py resume

# 마지막 수단: 운영자 책임 하 강제 재개
uv run python bot.py resume --force
```

리스크 사유(`KILL_SWITCH`, `DAILY_LOSS_LIMIT`, `POSITION_LOSS_LIMIT`)로 HALT된 경우 추가 규칙:

- 포지션/오픈오더 등 노출(exposure)이 남아 있으면 `resume`은 거부된다.
- 자동 청산은 수행되지 않으므로, 운영자가 먼저 노출을 수동으로 해소(포지션 평탄화/미체결 정리)해야 한다.
- 해소 후 `recovery-report`와 `health`를 다시 확인하고 `resume`을 실행한다.


예시 (`uv run python bot.py recovery-report`):

```text
[P2] resume_eligibility
  resume_allowed=0
  can_resume=false
  blockers=STARTUP_SAFETY_GATE_BLOCKED, HALT_RISK_OPEN_POSITION
  force_resume_allowed=0
```

## 6) 크래시 후 재개 전 필수 확인

크래시/강제 재시작 이후에는 아래를 모두 확인하기 전 재개하지 않는다.

1. `journalctl`로 마지막 예외 원인이 해소되었는지 확인
2. `uv run python bot.py recovery-report`에서 아래가 0인지 확인
   - `unresolved_orders`
   - `recovery_required_orders`
3. `uv run python bot.py reconcile` 후 다시 `recovery-report` 실행
4. live 모드면 거래소 오픈 주문/체결과 로컬 `orders/fills/trades` 샘플 대조
5. `uv run python bot.py health`에서 stale/error 이상 없음 확인
6. `uv run python bot.py resume`로 재개 후 30~60분 모니터링

## 7) 장애 대응 절차 (유형별)

### A. 재시작/프로세스 크래시

1. `sudo systemctl status bithumb-bot.service`로 재시작 루프 여부 확인.
2. `sudo journalctl -u bithumb-bot.service -n 200 --no-pager`로 직전 예외 확인.
3. 환경변수/설정 수정 후 `sudo systemctl restart bithumb-bot.service`.
4. 3~5분 모니터링 후 healthcheck 알림 미발생 확인.

### B. 중복 주문 의심

1. `uv run python bot.py orders --limit 100`로 최근 주문 상태 확인.
2. 동일 시점/동일 방향의 order가 중복인지 확인.
3. live 모드면 거래소 체결 내역과 `fills` 비교.
4. 필요시 봇 일시 중지: `uv run python bot.py pause`.
5. 수동 정리 후 재기동/재개.

### C. 잔고 불일치

1. `uv run python bot.py audit` 실행.
2. 불일치 시 `uv run python bot.py pnl --days 1` 및 `trades`/`fills` 대조.
3. live 모드면 브로커 잔고 API 기준으로 reconcile 수행.
4. 원인 확인 전 신규 주문 중단.

### D. 데이터 누락 (캔들 stale / sync 실패)

1. `uv run python bot.py health` 확인 (`last_candle_age_sec`).
2. `journalctl`에서 `sync failed`, `stale candle` 로그 확인.
3. 네트워크/API 상태 확인 후 `sudo systemctl restart bithumb-bot.service`.
4. 재발 시 `EVERY`, `INTERVAL`, rate limit 설정 완화.

### E. 레이트 리밋 대응

1. 에러 로그에서 HTTP 429/거래소 에러코드 확인.
2. `EVERY` 증가, 재시도/호출 빈도 완화.
3. healthcheck 알림 빈도가 높으면 임계치(`HEALTH_MAX_ERROR_COUNT`) 조정.
4. 복구 후 10~15분간 주문/체결/캔들 흐름 점검.

## 8) 알림 설정

하나 이상 설정하면 webhook 알림 사용, 미설정 시 콘솔 출력만 수행.

- Generic webhook: `NOTIFIER_WEBHOOK_URL`
- Slack incoming webhook: `SLACK_WEBHOOK_URL`
- Telegram bot: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`

권장:

- `NOTIFIER_ENABLED=true`
- `NOTIFIER_TIMEOUT_SEC=5`
- 비밀키/URL은 `/etc/bithumb-bot/bithumb-bot.env`에만 저장하고 로그에 출력 금지.

## 9) 백업 정책

- 기본 경로: `backups/`
- 기본 보관: 7일, 최대 30개
- 환경변수:
  - `BACKUP_DIR`
  - `BACKUP_RETENTION_DAYS`
  - `BACKUP_RETENTION_COUNT`

복구 예시:

```bash
sqlite3 data/bithumb_1m.sqlite ".restore backups/bithumb_1m.sqlite.20260101_120000.sqlite"
```

## 10) Live 모드 사전 점검 (fail-fast)

`MODE=live`로 시작하면 런타임 시작 전에 아래 항목을 강제 검증한다. 하나라도 누락되면 즉시 종료된다.

- `MAX_ORDER_KRW > 0`
- `MAX_DAILY_LOSS_KRW > 0`
- `MAX_DAILY_ORDER_COUNT > 0`
- `DB_PATH`는 `MODE=live`에서 반드시 명시해야 하며, 기본 경로 `data/bithumb_1m.sqlite` 사용 금지
- `LIVE_DRY_RUN=false`인 경우 `BITHUMB_API_KEY`, `BITHUMB_API_SECRET` 필수
- `LIVE_DRY_RUN=false`인 경우 `LIVE_REAL_ORDER_ARMED=true`를 명시해야 실주문 허용
- notifier는 반드시 활성/설정되어야 함(`NOTIFIER_WEBHOOK_URL` 또는 `SLACK_WEBHOOK_URL` 또는 `TELEGRAM_BOT_TOKEN`+`TELEGRAM_CHAT_ID`)
- `KILL_SWITCH_LIQUIDATE=true`는 현재 미지원(설정 시 기동 실패)

실주문 전환 절차(arming):

1. `LIVE_DRY_RUN=true` 상태로 로그/알림/복구 동작을 먼저 검증
2. 실주문 시작 직전에 `LIVE_DRY_RUN=false` + `LIVE_REAL_ORDER_ARMED=true`를 함께 설정
3. `LIVE_REAL_ORDER_ARMED=true`가 없으면 live preflight에서 즉시 종료(fail-fast)


Live 보수 preset 예시(소액 계정 + 알림 포함):

```bash
# 1) paper에서 먼저 검증 (DB 분리)
MODE=paper DB_PATH=data/paper.small.safe.sqlite \
NOTIFIER_ENABLED=true SLACK_WEBHOOK_URL=https://hooks.slack.com/services/xxx/yyy/zzz \
MAX_ORDER_KRW=30000 MAX_DAILY_LOSS_KRW=20000 MAX_DAILY_ORDER_COUNT=6 \
uv run python bot.py run

# 2) live dry-run (실주문 API 미호출)
MODE=live DB_PATH=data/live.small.safe.sqlite LIVE_DRY_RUN=true LIVE_REAL_ORDER_ARMED=false \
NOTIFIER_ENABLED=true SLACK_WEBHOOK_URL=https://hooks.slack.com/services/xxx/yyy/zzz \
MAX_ORDER_KRW=30000 MAX_DAILY_LOSS_KRW=20000 MAX_DAILY_ORDER_COUNT=6 \
uv run python bot.py run

# 3) 실주문 arming (운영자 명시 승인 후 직전에만)
MODE=live DB_PATH=data/live.small.safe.sqlite LIVE_DRY_RUN=false LIVE_REAL_ORDER_ARMED=true \
NOTIFIER_ENABLED=true SLACK_WEBHOOK_URL=https://hooks.slack.com/services/xxx/yyy/zzz \
MAX_ORDER_KRW=30000 MAX_DAILY_LOSS_KRW=20000 MAX_DAILY_ORDER_COUNT=6 \
BITHUMB_API_KEY=... BITHUMB_API_SECRET=... \
uv run python bot.py run
```
