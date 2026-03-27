# Operator Reporting Workflow (전략/거래/손익 관측)

이 문서는 운영자가 다음 질문을 빠르게 확인하기 위한 절차를 제공합니다.

- "얼마 벌었는가?"
- "왜 이런 주문/체결 판단이 나왔는가?"

핵심 명령은 `ops-report` 입니다.

## 1) 필요한 환경변수

최소 필수:

- `DB_PATH`: 조회할 SQLite DB 경로 (paper/live 분리 권장)

권장(컨텍스트 표시에 유용):

- `MODE` (`paper` / `live`)
- `PAIR` (예: `BTC_KRW`)
- `INTERVAL` (예: `1m`)
- `BITHUMB_ENV_FILE` 또는 `BITHUMB_ENV_FILE_LIVE` (AWS/systemd에서 env 주입 시)

> 경로 하드코딩 금지 원칙: DB 경로를 코드에 박아두지 말고 `DB_PATH` 또는 기존 env 파일 로딩 체계를 사용하세요.

## 2) 실행 방법

### 로컬

```bash
MODE=paper DB_PATH=data/paper.small.safe.sqlite uv run bithumb-bot ops-report --limit 20
```

### AWS (EC2/systemd 운영 환경)

env 파일(예: `/etc/bithumb-bot/live.env`)에 `DB_PATH`, `MODE`, `PAIR`, `INTERVAL`을 선언하고:

```bash
BITHUMB_ENV_FILE=/etc/bithumb-bot/live.env uv run bithumb-bot ops-report --limit 50
```

또는 서비스 계정으로 직접 실행:

```bash
sudo -u <service-user> BITHUMB_ENV_FILE=/etc/bithumb-bot/live.env uv run bithumb-bot ops-report --limit 50
```

기본 출력은 `stdout`이며, 파일 저장이 필요하면 운영자가 명시적으로 리다이렉트합니다.

```bash
BITHUMB_ENV_FILE=/etc/bithumb-bot/live.env uv run bithumb-bot ops-report --limit 100 > /tmp/ops-report.txt
```

## 3) 운영자 확인 절차

1. `ops-report` 실행
2. `[STRATEGY-SUMMARY]` 확인
   - 전략명(`strategy_context`)별 `order_count`, `fill_count`
   - `pnl_proxy = sell_notional - buy_notional - fee_total` 확인
3. `[RECENT-STRATEGY-ORDER-FILL-FLOW]` 확인
   - 최근 `order_events`를 시간순으로 읽어 판단/주문/체결 흐름 확인
   - `submission_reason_code`, `message(note)`로 판단 근거 확인
4. `[RECENT-TRADES-OPERATIONS]` 확인
   - `fee`, `cash_after`, `asset_after`, `note` 점검

## 4) 현재 스키마 기준 제약사항

현재 스키마에서는 `trades`에 `strategy_context`나 `client_order_id`가 없어, **전략별 확정 손익(realized PnL)** 을 정확히 분리 계산하기 어렵습니다.

따라서 현재 리포트는 전략 단위로 아래 대체 지표를 제공합니다.

- 주문 수(`order_count`)
- 체결 수(`fill_count`)
- 총 매수/매도 체결대금(`buy_notional`, `sell_notional`)
- 수수료 합계(`fee_total`)
- `pnl_proxy = sell - buy - fee`

## 5) 전략 판단 스냅샷 조회

전략 판단은 `strategy_decisions` 테이블에 저장됩니다. `context_json`에는 전략 계산 피처(SMA, 포지션 상태 등)를 JSON으로 보관해 사후 분석 시 재구성이 가능합니다.

```sql
SELECT
  decision_ts,
  strategy_name,
  signal,
  reason,
  candle_ts,
  market_price,
  confidence,
  context_json
FROM strategy_decisions
ORDER BY decision_ts DESC
LIMIT 50;
```

## 6) TODO (추가되면 좋은 필드)

- `trades.client_order_id` 또는 `trades.strategy_context`
  - 전략별 realized/unrealized PnL 정확 집계를 위해 필요
- 주문/체결과 판단 이벤트의 공통 correlation id
  - 장애 분석/감사 추적 속도 개선
