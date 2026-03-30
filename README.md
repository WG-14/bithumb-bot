# bithumb-bot

간단한 SMA 기반 빗썸 페이퍼/실거래 봇입니다.

## 빠른 시작

```bash
uv sync
uv run pytest -q
```

## CLI/엔트리포인트 (canonical)

- canonical CLI: `uv run bithumb-bot <command>`
  - `pyproject.toml`의 `project.scripts`에 등록된 공식 엔트리포인트입니다.
- 호환 엔트리포인트(동일 동작):
  - `uv run python -m bithumb_bot <command>`
  - `uv run python bot.py <command>`

## env 로딩 규칙 (코드 기준)

- 기본적으로 런타임은 `.env`를 **자동 로딩하지 않습니다**.
- env 파일 로딩은 `BITHUMB_ENV_FILE*` 계열을 명시했을 때만 수행됩니다.
  - `BITHUMB_ENV_FILE=/path/to/file.env` (최우선)
  - `MODE=live`이면 `BITHUMB_ENV_FILE_LIVE`
  - `MODE=paper`/`test`이면 `BITHUMB_ENV_FILE_PAPER`
- healthcheck 스크립트는 fail-fast 정책으로 **명시적 env 파일이 없으면 실패**합니다.

로컬에서 `.env`를 쓰려면 명시적으로 지정하세요.

```bash
BITHUMB_ENV_FILE=.env uv run bithumb-bot health
```

## 자주 쓰는 명령

```bash
uv run bithumb-bot sync
uv run bithumb-bot ticker
uv run bithumb-bot candles --limit 5
uv run bithumb-bot signal --short 7 --long 30
uv run bithumb-bot explain --short 7 --long 30
uv run bithumb-bot status
uv run bithumb-bot trades --limit 20
uv run bithumb-bot ops-report --limit 20
uv run bithumb-bot run --short 7 --long 30
```

- 운영자 전략/손익 검증 절차: `docs/OPERATOR_REPORTING.md`

## 경로 정책 (PathManager 기준)

- 저장 규칙 기준 문서:
  - `docs/storage-layout.md`
  - `docs/runtime-data-policy.md`
- 경로는 env(`ENV_ROOT`, `RUN_ROOT`, `DATA_ROOT`, `LOG_ROOT`, `BACKUP_ROOT`, `ARCHIVE_ROOT`)로 주입하고, 하위 구조(`run/<mode>`, `data/<mode>/*`, `logs/<mode>/*`, `backup/<mode>/*`)는 코드(PathManager)가 강제합니다.
- `DB_PATH`, `RUN_LOCK_PATH`, `BACKUP_DIR`는 **점진적 호환용 override**로 유지됩니다.
  - `DB_PATH` 미설정 시: `DATA_ROOT/<mode>/trades/<mode>.sqlite`
  - `RUN_LOCK_PATH` 미설정 시: `RUN_ROOT/<mode>/bithumb-bot.lock`
  - `BACKUP_DIR` 미설정 시: `BACKUP_ROOT/<mode>/db`
- `MODE=live`에서는 위 루트 변수들이 필수이며, repo 내부 경로/상대경로는 fail-fast로 차단됩니다.
- `MODE=paper`에서는 로컬 개발 편의를 위해 상대경로 루트를 허용하되, 운영 배포에서는 live와 동일하게 절대경로를 권장합니다.

## run lock 동작

- `run` 명령은 시작 시 run lock을 획득하며, 이미 다른 run loop가 실행 중이면 즉시 실패합니다.
- lock 경로는 `RUN_LOCK_PATH`(미설정 시 `RUN_ROOT/<mode>/bithumb-bot.lock`)입니다.
- lock 충돌 시 현재 owner PID/host/생성시각/lock age 정보를 포함해 에러를 출력합니다.
- native Windows에서는 `fcntl` 미지원으로 run lock이 동작하지 않으며, 에러 메시지대로 WSL/Linux에서 실행해야 합니다.

## 주요 환경 변수

현재 코드에서 실제로 사용하는 주요 옵션입니다.

- `MODE` (기본: `paper`)
- `PAIR` (기본: `BTC_KRW`)
- `INTERVAL` (기본: `1m`)
- `EVERY` (기본: `60`)
- `STRATEGY_NAME` (기본: `sma_with_filter`)
- `SMA_SHORT` (기본: `7`)
- `SMA_LONG` (기본: `30`)
- `COOLDOWN_MIN` (기본: `1`)
- `MIN_GAP` (기본: `0.0003`)
- `DB_PATH` (기본: `data/bithumb_1m.sqlite`)
- `LIVE_MIN_ORDER_QTY` (기본: `0`, 0이면 비활성)
- `LIVE_ORDER_QTY_STEP` (기본: `0`, 0이면 비활성)
- `LIVE_ORDER_MAX_QTY_DECIMALS` (기본: `0`, 0이면 비활성)

> `ENTRY_MODE`, `advise` 커맨드 같은 과거 옵션/명령은 현재 CLI에서 사용하지 않습니다.

전략 선택은 전부 환경변수 주입(`STRATEGY_NAME`) 기반이며, 런타임/배포 환경(AWS EC2/ECS/Lambda 등)에서 파일 경로 하드코딩 없이 동일하게 동작합니다. 운영 기본값은 체결 비용/노이즈를 고려한 `sma_with_filter`이며, 백테스트/비교가 필요하면 `STRATEGY_NAME=sma_cross`로 즉시 override할 수 있습니다(대소문자/공백 입력도 정규화되어 동작).

예시(AWS 배포 환경변수만으로 전략 전환):

```bash
# 운영 기본(코드 수정 없음)
STRATEGY_NAME=sma_with_filter

# 비교/백테스트 호환 모드
STRATEGY_NAME=sma_cross
```

## Live 모드(실거래)

- `MODE=live`로 실행하면 paper와 동일한 `orders/fills/trades/portfolio` 원장 스키마를 사용합니다.
- `MODE=live`에서는 `DB_PATH`를 반드시 명시해야 하며, 기본값 `data/bithumb_1m.sqlite`(paper와 공유될 수 있는 경로)는 사용할 수 없습니다.
- `MODE=live` preflight는 paper/test 성격의 혼합 설정을 거부합니다. 예: 기본/공유 DB 경로, paper 전용 키(`START_CASH_KRW`, `BUY_FRACTION`, `FEE_RATE`, `PAPER_FEE_RATE`, `PAPER_FEE_RATE_ESTIMATE`, `SLIPPAGE_BPS`)가 설정된 경우, 또는 live 보호값(`MAX_ORDER_KRW`, `MAX_DAILY_LOSS_KRW`, `MAX_DAILY_ORDER_COUNT`, `MAX_ORDERBOOK_SPREAD_BPS`, `MAX_MARKET_SLIPPAGE_BPS`, `LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS`)이 유효한 값(> 0, 유한값)으로 설정되지 않은 경우 기동 전에 fail-fast로 차단됩니다.
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
MODE=live DATA_ROOT=/var/lib/bithumb-bot/data RUN_ROOT=/var/lib/bithumb-bot/run LOG_ROOT=/var/lib/bithumb-bot/logs BACKUP_ROOT=/var/lib/bithumb-bot/backup ENV_ROOT=/var/lib/bithumb-bot/env \
DB_PATH=/var/lib/bithumb-bot/data/live/trades/live.small.safe.sqlite LIVE_DRY_RUN=true LIVE_REAL_ORDER_ARMED=false \
MAX_ORDER_KRW=30000 MAX_DAILY_LOSS_KRW=20000 MAX_DAILY_ORDER_COUNT=6 \
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/xxx/yyy/zzz \
uv run bithumb-bot run
```

실주문 전환(운영자 명시 승인 후에만):

```bash
MODE=live DATA_ROOT=/var/lib/bithumb-bot/data RUN_ROOT=/var/lib/bithumb-bot/run LOG_ROOT=/var/lib/bithumb-bot/logs BACKUP_ROOT=/var/lib/bithumb-bot/backup ENV_ROOT=/var/lib/bithumb-bot/env \
DB_PATH=/var/lib/bithumb-bot/data/live/trades/live.small.safe.sqlite LIVE_DRY_RUN=false LIVE_REAL_ORDER_ARMED=true \
MAX_ORDER_KRW=30000 MAX_DAILY_LOSS_KRW=20000 MAX_DAILY_ORDER_COUNT=6 \
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/xxx/yyy/zzz \
BITHUMB_API_KEY=... BITHUMB_API_SECRET=... \
uv run bithumb-bot run
```

paper/live DB 분리 예시:

```bash
# paper 검증
MODE=paper DB_PATH=data/paper.small.safe.sqlite uv run bithumb-bot run

# live 검증/운영
MODE=live DB_PATH=data/live.small.safe.sqlite uv run bithumb-bot run
```
- 안전장치: `MAX_ORDER_KRW`, `MAX_DAILY_LOSS_KRW`, `MAX_DAILY_ORDER_COUNT`, `KILL_SWITCH`.
- 재시작 시 엔진이 `reconcile`을 수행하여 열린 주문/체결/포트폴리오를 동기화합니다.

### 보수적 라이브 프로필 (권장: 1,000,000 KRW 계정)

초기 실거래는 아래처럼 **작게 시작**하는 것을 권장합니다.

- `MAX_ORDER_KRW=30000` (주문 1회당 약 3%)
- `MAX_DAILY_LOSS_KRW=20000` (일 손실 약 2%에서 즉시 HALT(무기한 중지, 자동 재개 없음))
- `MAX_DAILY_ORDER_COUNT=6` (과매매 방지)
- `KILL_SWITCH=false` (비상시에만 true)
- `KILL_SWITCH_LIQUIDATE=false` (평시 off. 필요 시 `true`로 설정하면 kill switch 동작 시 포지션 flatten을 추가로 시도합니다. live preflight 실패 사유는 아닙니다.)
- 일 손실 한도 초과 시 엔진은 신규 주문 전에 거래를 **HALT**하고 오픈주문 취소 + 포지션 flatten을 시도한 뒤, 노출/미해결 상태가 남으면 운영자 복구/재개 승인을 요구합니다.
- `LIVE_DRY_RUN=true`로 최소 반나절 이상 검증 후 `false` 전환

## 라이브 시작 전 체크리스트 (Startup)

1. `BITHUMB_ENV_FILE`(또는 `BITHUMB_ENV_FILE_LIVE`)가 가리키는 env 파일에 라이브 안전값이 반영되었는지 확인
2. `uv run bithumb-bot health`에서 `trading_enabled=True`, `error_count` 낮음, `last_candle_age_sec` 정상 확인
3. `uv run bithumb-bot recovery-report`에서 미해결 주문/복구 필요 건수와 오래된 미해결 주문 요약(top 5) 확인
4. 처음 라이브 전환 시 `MODE=live`, `LIVE_DRY_RUN=true`로 기동 후 로그/알림 확인
5. API 키를 활성화하기 전 `pause/resume/reconcile` 명령이 정상 동작하는지 점검
6. 실주문 전환(`LIVE_DRY_RUN=false`) 직후 30~60분 수동 모니터링

## 비상 정지 / 일시중지 / 복구

```bash
# 즉시 신규 거래 중지
uv run bithumb-bot pause

# 상태 점검
uv run bithumb-bot recovery-report
uv run bithumb-bot health

# (live) 원격 오픈 주문 일괄 취소
uv run bithumb-bot cancel-open-orders

# 정합성 점검
uv run bithumb-bot reconcile

# 보수적 재개(문제 있으면 자동 거부)
uv run bithumb-bot resume
```

- 긴급 시에는 `pause`를 먼저 실행하고, 원인 파악 전 `resume --force`는 피하세요.
- `KILL_SWITCH=true`는 마지막 안전장치로 사용하고, 해제 전 반드시 주문/체결 정합성을 다시 확인하세요.

## 크래시 후 재개 전 필수 확인

1. `journalctl -u bithumb-bot.service -n 200 --no-pager`로 마지막 예외/네트워크 오류 원인 확인
2. `uv run bithumb-bot recovery-report`에서 `unresolved_orders`, `recovery_required_orders`가 0인지 확인 (0이 아니면 오래된 주문 요약 목록으로 우선 대응 대상 확인)
3. `uv run bithumb-bot reconcile` 실행 후 다시 `recovery-report` 확인
4. live 모드면 거래소 오픈 주문/체결 내역과 로컬 `orders/fills`가 일치하는지 샘플 대조
5. `uv run bithumb-bot health` 정상 확인 후 `uv run bithumb-bot resume`

## 24/7 운영(systemd + healthcheck + backup)

- systemd 유닛: `deploy/systemd/`
  - `bithumb-bot.service` (`Restart=always`)
  - `bithumb-bot-healthcheck.timer` (1분 주기)
  - `bithumb-bot-backup.timer` (6시간 주기)
- 운영 절차 문서: `docs/RUNBOOK.md`
- 제한적 무인 운용 체크리스트(요약): `docs/LIMITED_UNATTENDED_CHECKLIST.md`
- 백업 스크립트: `scripts/backup_sqlite.sh`
- 세 유닛(`bithumb-bot.service`, `bithumb-bot-healthcheck.service`, `bithumb-bot-backup.service`) 모두 `BITHUMB_ENV_FILE=@BITHUMB_ENV_FILE_LIVE@`를 사용하도록 템플릿이 작성되어 있습니다. 설치 시 `render_units.sh`로 실제 경로를 치환한 뒤 배포하세요.
- `bithumb-bot-healthcheck.service`는 비대화형 systemd PATH 의존성을 제거하기 위해 `BITHUMB_UV_BIN`(기본값: 렌더 시점 `command -v uv` 결과, 없으면 `uv`)을 사용합니다.
- systemd 실행 계정은 `BITHUMB_RUN_USER`로 주입할 수 있으며, 기본값은 유닛 렌더링을 실행한 사용자(`id -un`)입니다.
- `bithumb-bot.service` / `bithumb-bot-paper.service`는 `PYTHONUNBUFFERED=1`과 `python -u`를 함께 사용해 journald에서 `[RUN]`/`[SKIP]` 로그가 지연 버퍼링 없이 바로 보이도록 구성합니다.

빠른 확인:

```bash
sudo systemctl restart bithumb-bot.service
uv run bithumb-bot health
./scripts/backup_sqlite.sh
```

## 실행 환경 지원 범위

- 권장/지원: Linux (예: Ubuntu, AWS EC2 Linux)
  - systemd 운영은 Linux에서만 전제합니다.
- Windows:
  - native Windows는 run lock(`fcntl`) 미지원으로 `run` 루프 운영 대상이 아닙니다.
  - 개발/실행은 WSL2(Linux 사용자공간)에서 수행하세요.
