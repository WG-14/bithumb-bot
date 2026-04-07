# Live Incident Runbook

## 목적
live 운영 중 장애 발생 시, 감정적으로 대응하지 않고 같은 순서로 점검/조치하기 위한 문서.

## 1. 먼저 확인
1. `sudo systemctl status bithumb-bot.service --no-pager`
2. `sudo journalctl -u bithumb-bot.service -n 100 --no-pager`
3. `sudo systemctl status bithumb-bot-healthcheck.timer --no-pager`
4. `./scripts/check_live_runtime.sh`

## 2. 즉시 중단이 필요한 경우
다음 중 하나면 즉시 운영 중단 검토:
- 반복적인 예외 발생
- 주문/체결/상태 불일치 의심
- halt 상태 반복
- 예상하지 않은 실주문 가능성
- DB 손상 의심

## 3. 중단 절차
1. 서비스 중지
   - `sudo systemctl stop bithumb-bot.service`
2. 상태 확인
   - `sudo systemctl status bithumb-bot.service --no-pager`
3. 스냅샷 수집
   - `./scripts/collect_live_snapshot.sh`

## 4. DB 백업
- 운영 중단 직후 DB 백업 수행
- 기존 backup timer와 별도로 수동 백업도 남긴다

예:
- `cp /var/lib/bithumb-bot/data/live/trades/live.sqlite /var/lib/bithumb-bot/backup/live/db/live.manual.$(date +%Y%m%d_%H%M%S).sqlite`

## 5. 원인 분석 기본 축
- 최근 journal 에러
- healthcheck 결과
- restart 직전/직후 상태
- reconcile 관련 로그
- notifier 경고 여부
- 최근 DB/backup 상태

## 6. 재기동 전 체크
- [ ] 원인 파악 여부
- [ ] 같은 오류 즉시 재발 가능성 검토
- [ ] env 설정 이상 없음
- [ ] lock/db 상태 이상 없음
- [ ] 필요 시 restore 여부 판단

## 7. 재기동
1. `sudo systemctl start bithumb-bot.service`
2. `sudo systemctl status bithumb-bot.service --no-pager`
3. `sudo journalctl -u bithumb-bot.service -n 100 --no-pager`

## 8. 사후 기록
아래 항목 기록:
- 발생 시각
- 증상
- 영향 범위
- 원인
- 조치
- 재발 방지책
## 9. Dust Residual vs Unresolved Order

- `dust_state=matched_harmless_dust` means broker/local dust matches closely enough to be interpreted as harmless dust. That label can still mean a real BTC remainder exists; it is an operator reading of a small remainder, not a literal zero-balance claim. Resume is allowed only if `resume_allowed_by_policy=1`.
- `dust_state=dangerous_dust` means the remainder is not safely resumable. Treat it as an operator-review condition before any resume or new orders.
- `unresolved_count > 0` or `recovery_required_count > 0` means order consistency is still unresolved. Treat that as a recovery problem first, not a dust problem.
- If a manual app sell happened while the bot was stopped, do not restart immediately. Re-run `health`, `recovery-report`, and `ops-report` first.
- If reports show below-minimum dust only, avoid forcing another liquidation attempt unless exchange minimum quantity and minimum notional are both clearly satisfied after qty-step/decimal normalization.
- Incident triage order:
  1. Check `recovery-report [P2]`. If `resume_allowed=0` and `can_resume=false`, restart stays blocked.
  2. If the blocker includes `MATCHED_DUST_POLICY_REVIEW_REQUIRED` or `DANGEROUS_DUST_REVIEW_REQUIRED`, decide whether this is matched dust under policy review or dangerous dust requiring manual intervention.
  3. Only treat the position as dust-only when `unresolved_count=0`, `recovery_required_count=0`, `dust_state=matched_harmless_dust`, and `resume_allowed_by_policy=1`.
- Manual dust review checklist:
  1. app side: read `dust_state`, `dust_action`, `dust_resume_allowed_by_policy`, and `recent_dust_unsellable_event`
  2. DB side: confirm there is no unresolved or recovery-required order left
  3. broker side: confirm the remaining balance reflected by `dust_broker_qty`
  4. exchange minimums: check both `dust_min_qty` and `dust_min_notional_krw`
- Do not make manual app liquidation the default dust workflow. Use it only as an exception after reports, broker balance, and DB state have been compared.
- `accounts_flat_start_allowed` is not resume permission. It is only an `/v1/accounts` diagnostic and must not override `recovery-report` restart blockers.
- `order_rules_autosync=FALLBACK` means `/v1/orders/chance` rule data was not available and the bot is using local fallback constraints. In live mode, clear that warning before real-order arming.
- `/v2/orders` pre-validation still applies after the rule snapshot is loaded: market buys are `side=bid, order_type=price, price=<KRW>`, and market sells are `side=ask, order_type=market, volume=<qty>`.
