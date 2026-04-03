# Operator Reporting Workflow (전략/거래/손익 관측)

이 문서는 운영자가 다음 질문을 빠르게 확인하기 위한 절차를 제공합니다.

- "얼마 벌었는가?"
- "왜 이런 주문/체결 판단이 나왔는가?"

핵심 명령은 `ops-report`, `fee-diagnostics`, `experiment-report` 입니다.

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
MODE=paper DB_PATH=/var/lib/bithumb-bot/data/paper/trades/paper.small.safe.sqlite uv run bithumb-bot ops-report --limit 20
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

## 3-1) 수수료 반영 진단 (`fee-diagnostics`)

실제 체결 수수료 반영 상태를 빠르게 검증하려면 `fee-diagnostics`를 사용합니다.

### 제공 지표

- 최근 N개 fill의 평균 수수료율(`average_fee_rate`)
- `fee=0` fill 개수/비율
- 평균/중앙값 fee bps
- 추정 수수료율(`--estimated-fee-rate` 또는 `FEE_RATE`) 대비 실제 수수료율 차이(bps)
- 최근 왕복 거래(`trade_lifecycles`) 기준 총 수수료
- 수수료 반영 전/후 PnL 비교(`gross_pnl` vs `net_pnl`)

### 실행 예시

```bash
# 사람이 읽기 쉬운 텍스트 리포트
MODE=live DB_PATH=/var/lib/bithumb-bot/live.sqlite \
  uv run bithumb-bot fee-diagnostics --fill-limit 200 --roundtrip-limit 100

# JSON 출력 (외부 모니터링/대시보드 적재 용도)
MODE=live DB_PATH=/var/lib/bithumb-bot/live.sqlite \
  uv run bithumb-bot fee-diagnostics --fill-limit 200 --roundtrip-limit 100 --json
```

출력은 기본적으로 `stdout`만 사용합니다. 파일 저장이 필요하면 운영 환경에서 리다이렉트를 사용하세요.

## 4) 현재 스키마 기준 제약사항

`strategy-report`는 `trade_lifecycles`의 canonical linkage(`entry/exit trade id`, `entry/exit fill id`, `strategy_name`)를 사용해 **전략별 확정 손익(realized PnL)** 을 직접 집계합니다.

`ops-report`의 `strategy_summary`는 여전히 intent/fill 기반 참고치이며, `pnl_proxy_deprecated`(legacy 참고 지표)를 포함합니다. 운영/검증 시 핵심 판단은 `strategy-report`의 realized 지표를 우선 사용하세요.

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

- 주문/체결과 판단 이벤트의 공통 correlation id
  - 장애 분석/감사 추적 속도 개선

## 7) 전략 실험 비교 리포트 (`strategy-report`)

`trade_lifecycles` 기반으로 전략별 성과를 비교합니다. 기본 출력은 `stdout`이며, JSON 응답이 필요하면 `--json`을 사용합니다.

### 제공 지표

- `trade_count`
- `win_rate`
- `average_gain`
- `average_loss`
- `realized_gross_pnl`
- `fee_total`
- `realized_net_pnl` (`net_pnl` 호환 필드도 JSON에 유지)
- `expectancy_per_trade`
- `holding_time` 요약(`avg/min/max` 초)
- reason linkage 요약(`entry_reason_linked_count`, `exit_reason_linked_count`, sample)

### 집계 축/필터

- 집계 축(`--group-by`): `strategy_name`, `exit_rule_name`, `pair`
- 필터: `--strategy-name`, `--exit-rule-name`, `--pair`, `--from-date`, `--to-date`
  - 날짜는 KST `YYYY-MM-DD` 형식, `trade_lifecycles.exit_ts` 기준으로 필터링됩니다.

### 실행 예시

```bash
# 기본: 전략명 + 청산 규칙 기준 집계
MODE=paper DB_PATH=/var/lib/bithumb-bot/data/paper/trades/paper.sqlite uv run bithumb-bot strategy-report

# 기간 + 마켓 필터 + JSON 출력
MODE=paper DB_PATH=/var/lib/bithumb-bot/data/paper/trades/paper.sqlite \
  uv run bithumb-bot strategy-report \
  --from-date 2026-03-01 --to-date 2026-03-27 \
  --pair BTC_KRW \
  --group-by strategy_name,exit_rule_name,pair \
  --json
```

데이터가 부족하거나 필터에 일치하는 거래가 없으면 실패(exit non-zero) 대신 설명 가능한 메시지를 출력합니다.

## 8) 소액 live 기대값 검증 리포트 (`experiment-report`)

`experiment-report`는 운영 안정성 지표(`ops-report`/`health`/`recovery-report`)와 분리된 **실험 해석용 리포트**입니다.  
특히 "10,000 KRW 소액 live 실험에서 현재 전략의 기대값이 있는가?"를 보수적으로 판단하기 위한 지표를 제공합니다.

### 제공 지표

- `realized_net_pnl`
- `trade_count` (sample size)
- `win_rate`
- `expectancy_per_trade`
- `max_drawdown_proxy` (trade 순서 누적 손익 기준)
- `top-N concentration` (소수 거래 의존도)
- `longest_losing_streak`
- `time-of-day bucket performance`
- `market regime bucket performance` (`volatility`/`overextension` 버킷 조합)
  - `trade_count_share`
  - `realized_net_pnl_share`
  - `absolute_pnl_concentration` (|pnl| 기준 레짐 집중도)
  - `profitable_pnl_concentration` / `loss_pnl_concentration`

### 경고 규칙

- 표본 부족: `insufficient sample`
- 상위 거래 의존도 높음: `concentrated pnl`
- 특정 레짐 편중: `regime skew`
- 특정 레짐에 pnl 기여가 과도 집중: `regime pnl skew`

### 실행 예시

```bash
MODE=live DB_PATH=/var/lib/bithumb-bot/data/live/trades/live.small.safe.sqlite \
  uv run bithumb-bot experiment-report \
  --from-date 2026-03-01 --to-date 2026-03-31 \
  --sample-threshold 30 \
  --top-n 3 \
  --concentration-threshold 0.60 \
  --regime-skew-threshold 0.70 \
  --regime-pnl-skew-threshold 0.70
```

JSON 출력이 필요하면 `--json`을 사용합니다.
