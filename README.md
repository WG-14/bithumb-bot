# bithumb-bot

간단한 SMA 기반 빗썸 페이퍼/실거래 봇입니다.

## 빠른 시작

```bash
uv sync
# 로컬 개발 시에만 .env를 사용하세요. 배포/서버에서는 환경변수를 외부에서 주입하는 것을 권장합니다.
cp .env.example .env  # 없으면 생략 가능
uv run pytest -q
```

## 자주 쓰는 명령

```bash
uv run python bot.py sync
uv run python bot.py ticker
uv run python bot.py candles --limit 5
uv run python bot.py signal --short 7 --long 30
uv run python bot.py explain --short 7 --long 30
uv run python bot.py status
uv run python bot.py trades --limit 20
uv run python bot.py run --short 7 --long 30
```

## 주요 환경 변수

현재 코드에서 실제로 사용하는 주요 옵션입니다.

- `MODE` (기본: `paper`)
- `PAIR` (기본: `BTC_KRW`)
- `INTERVAL` (기본: `1m`)
- `EVERY` (기본: `60`)
- `SMA_SHORT` (기본: `7`)
- `SMA_LONG` (기본: `30`)
- `COOLDOWN_MIN` (기본: `1`)
- `MIN_GAP` (기본: `0.0003`)
- `DB_PATH` (기본: `data/bithumb_1m.sqlite`)
- `LIVE_MIN_ORDER_QTY` (기본: `0`, 0이면 비활성)
- `LIVE_ORDER_QTY_STEP` (기본: `0`, 0이면 비활성)
- `LIVE_ORDER_MAX_QTY_DECIMALS` (기본: `0`, 0이면 비활성)

> `ENTRY_MODE`, `advise` 커맨드 같은 과거 옵션/명령은 현재 CLI에서 사용하지 않습니다.

## Live 모드(실거래)

- `MODE=live`로 실행하면 paper와 동일한 `orders/fills/trades/portfolio` 원장 스키마를 사용합니다.
- `MODE=live`에서는 `DB_PATH`를 반드시 명시해야 하며, 기본값 `data/bithumb_1m.sqlite`(paper와 공유될 수 있는 경로)는 사용할 수 없습니다.
- `MODE=live`에서는 notifier가 반드시 활성/설정되어 있어야 합니다 (`NOTIFIER_WEBHOOK_URL` 또는 `SLACK_WEBHOOK_URL` 또는 `TELEGRAM_BOT_TOKEN`+`TELEGRAM_CHAT_ID`). 미설정 시 기동이 실패합니다.
- `LIVE_DRY_RUN=true`를 켜면 주문 API 호출 없이 동일 경로로 주문/로그 처리만 수행합니다.
- 실주문(`LIVE_DRY_RUN=false`)은 `LIVE_REAL_ORDER_ARMED=true`를 명시적으로 설정한 경우에만 허용됩니다.

### 실주문 arming 방법

1. 먼저 `LIVE_DRY_RUN=true`로 충분히 검증합니다.
2. 실주문 직전에만 아래 값을 함께 설정합니다.
   - `LIVE_DRY_RUN=false`
   - `LIVE_REAL_ORDER_ARMED=true`
3. 둘 중 하나라도 누락되면 기동 시 fail-fast로 즉시 종료됩니다.

예시(보수적 소액 계정 live dry-run + notifier):

```bash
MODE=live DB_PATH=data/live.small.safe.sqlite LIVE_DRY_RUN=true LIVE_REAL_ORDER_ARMED=false \
MAX_ORDER_KRW=30000 MAX_DAILY_LOSS_KRW=20000 MAX_DAILY_ORDER_COUNT=6 \
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/xxx/yyy/zzz \
uv run python bot.py run
```

실주문 전환(운영자 명시 승인 후에만):

```bash
MODE=live DB_PATH=data/live.small.safe.sqlite LIVE_DRY_RUN=false LIVE_REAL_ORDER_ARMED=true \
MAX_ORDER_KRW=30000 MAX_DAILY_LOSS_KRW=20000 MAX_DAILY_ORDER_COUNT=6 \
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/xxx/yyy/zzz \
BITHUMB_API_KEY=... BITHUMB_API_SECRET=... \
uv run python bot.py run
```

paper/live DB 분리 예시:

```bash
# paper 검증
MODE=paper DB_PATH=data/paper.small.safe.sqlite uv run python bot.py run

# live 검증/운영
MODE=live DB_PATH=data/live.small.safe.sqlite uv run python bot.py run
```
- 안전장치: `MAX_ORDER_KRW`, `MAX_DAILY_LOSS_KRW`, `MAX_DAILY_ORDER_COUNT`, `KILL_SWITCH`.
- 재시작 시 엔진이 `reconcile`을 수행하여 열린 주문/체결/포트폴리오를 동기화합니다.

### 보수적 라이브 프로필 (권장: 1,000,000 KRW 계정)

초기 실거래는 아래처럼 **작게 시작**하는 것을 권장합니다.

- `MAX_ORDER_KRW=30000` (주문 1회당 약 3%)
- `MAX_DAILY_LOSS_KRW=20000` (일 손실 약 2%에서 즉시 HALT(무기한 중지, 자동 재개 없음))
- `MAX_DAILY_ORDER_COUNT=6` (과매매 방지)
- `KILL_SWITCH=false` (비상시에만 true)
- `KILL_SWITCH_LIQUIDATE=false` (**청산 모드 미구현**. 반드시 false 유지; true면 live preflight 실패)
- 일 손실 한도 초과 시 엔진은 신규 주문 전에 거래를 **영구 HALT**하고 오픈주문 취소만 1회 시도합니다(자동 재개/강제 청산 없음).
- `LIVE_DRY_RUN=true`로 최소 반나절 이상 검증 후 `false` 전환

## 라이브 시작 전 체크리스트 (Startup)

1. `.env` 또는 `/etc/bithumb-bot/bithumb-bot.env`에 라이브 안전값이 반영되었는지 확인
2. `uv run python bot.py health`에서 `trading_enabled=True`, `error_count` 낮음, `last_candle_age_sec` 정상 확인
3. `uv run python bot.py recovery-report`에서 미해결 주문/복구 필요 건수와 오래된 미해결 주문 요약(top 5) 확인
4. 처음 라이브 전환 시 `MODE=live`, `LIVE_DRY_RUN=true`로 기동 후 로그/알림 확인
5. API 키를 활성화하기 전 `pause/resume/reconcile` 명령이 정상 동작하는지 점검
6. 실주문 전환(`LIVE_DRY_RUN=false`) 직후 30~60분 수동 모니터링

## 비상 정지 / 일시중지 / 복구

```bash
# 즉시 신규 거래 중지
uv run python bot.py pause

# 상태 점검
uv run python bot.py recovery-report
uv run python bot.py health

# (live) 원격 오픈 주문 일괄 취소
uv run python bot.py cancel-open-orders

# 정합성 점검
uv run python bot.py reconcile

# 보수적 재개(문제 있으면 자동 거부)
uv run python bot.py resume
```

- 긴급 시에는 `pause`를 먼저 실행하고, 원인 파악 전 `resume --force`는 피하세요.
- `KILL_SWITCH=true`는 마지막 안전장치로 사용하고, 해제 전 반드시 주문/체결 정합성을 다시 확인하세요.

## 크래시 후 재개 전 필수 확인

1. `journalctl -u bithumb-bot.service -n 200 --no-pager`로 마지막 예외/네트워크 오류 원인 확인
2. `uv run python bot.py recovery-report`에서 `unresolved_orders`, `recovery_required_orders`가 0인지 확인 (0이 아니면 오래된 주문 요약 목록으로 우선 대응 대상 확인)
3. `uv run python bot.py reconcile` 실행 후 다시 `recovery-report` 확인
4. live 모드면 거래소 오픈 주문/체결 내역과 로컬 `orders/fills`가 일치하는지 샘플 대조
5. `uv run python bot.py health` 정상 확인 후 `uv run python bot.py resume`

## 24/7 운영(systemd + healthcheck + backup)

- systemd 유닛: `deploy/systemd/`
  - `bithumb-bot.service` (`Restart=always`)
  - `bithumb-bot-healthcheck.timer` (1분 주기)
  - `bithumb-bot-backup.timer` (6시간 주기)
- 운영 절차 문서: `docs/RUNBOOK.md`
- 제한적 무인 운용 체크리스트(요약): `docs/LIMITED_UNATTENDED_CHECKLIST.md`
- 백업 스크립트: `scripts/backup_sqlite.sh`

빠른 확인:

```bash
sudo systemctl restart bithumb-bot.service
uv run python bot.py health
./scripts/backup_sqlite.sh
```
