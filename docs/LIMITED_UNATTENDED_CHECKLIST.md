# Limited Unattended Live Ops Checklist (Bithumb BTC)

짧은 운영용 체크리스트입니다. 범위는 **Bithumb BTC 단일 전략, 제한적 무인 운용(수시 수동 점검 전제)** 입니다.

> 현재 운영 모델은 완전 24/7 자율운용이 아니라, **자동 재시작 + 보수적 HALT + 운영자 재정합/재개 승인**을 전제로 한 pilot 단계입니다.

## 1) 모드/상태 분리 (시작 전 1분)

- [ ] 오늘 세션 모드 명시: `paper` / `live dry-run` / `live armed`
- [ ] `paper`와 `live`는 **서로 다른 `DB_PATH`** 사용
- [ ] live 실주문은 `LIVE_DRY_RUN=false` + `LIVE_REAL_ORDER_ARMED=true` 동시 설정일 때만 허용

빠른 확인 예시:

```bash
MODE=paper DB_PATH=data/paper.safe.sqlite uv run python bot.py health
MODE=live DB_PATH=data/live.safe.sqlite LIVE_DRY_RUN=true uv run python bot.py health
```

## 2) live preflight (실주문 전 고정 순서)

```bash
uv run python bot.py broker-diagnose
uv run python bot.py health
uv run python bot.py recovery-report
uv run python bot.py reconcile
uv run python bot.py recovery-report
```

통과 조건:

- `broker-diagnose`의 `overall=PASS`
- `health`에 stale/error 이상 없음
- `recovery-report`에서 unresolved / recovery-required 정리 완료

주의:

- `.env.example` 복사만으로는 live preflight를 통과하지 못할 수 있다. live에서 `DB_PATH` 명시, `MAX_ORDER_KRW/MAX_DAILY_LOSS_KRW/MAX_DAILY_ORDER_COUNT > 0`, `MAX_ORDERBOOK_SPREAD_BPS/MAX_MARKET_SLIPPAGE_BPS/LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS > 0`(유한값), notifier 설정이 필요하다.
- live에서는 paper 전용 키(`START_CASH_KRW`, `BUY_FRACTION`, `FEE_RATE`, `SLIPPAGE_BPS`)가 설정되어 있으면 preflight가 거부된다.
- API 키 권한/출금 비활성/IP whitelist 상태는 코드 자동 검증이 아니라 수동 점검 항목이다.

## 3) API 키/권한 + notifier 체크

- [ ] Bithumb API 키: 조회 + 주문 권한 확인 (출금 권한 비활성 권장, IP whitelist 포함 스코프는 수동 점검)
- [ ] notifier 필수 설정 확인 (`NOTIFIER_WEBHOOK_URL` 또는 `SLACK_WEBHOOK_URL` 또는 Telegram 조합)
- [ ] 알림 채널에 최근 health/recovery 알림이 정상 도착하는지 점검

## 4) 운영 중 즉시 제어 (halt/recovery/resume)

```bash
# 신규 거래 즉시 중단
uv run python bot.py pause

# (live) 원격 미체결 정리
uv run python bot.py cancel-open-orders

# 정합성 재확인
uv run python bot.py reconcile
uv run python bot.py recovery-report
```

재개:

```bash
uv run python bot.py resume
```

- `resume` 거부 시 먼저 `recovery-report` blocker를 해소
- `resume --force`는 마지막 수단으로만 사용

비상 노출 축소(필요 시):

```bash
uv run python bot.py flatten-position --dry-run
uv run python bot.py flatten-position
```

## 5) 재시작 후 검증 플로우 (restart/reconcile/recovery/resume)

```bash
uv run python bot.py restart-checklist
uv run python bot.py health
uv run python bot.py recovery-report
uv run python bot.py reconcile
uv run python bot.py recovery-report
# 필요 시
uv run python bot.py cancel-open-orders
uv run python bot.py reconcile
uv run python bot.py recovery-report
uv run python bot.py resume
```

판정:

- `restart-checklist`의 `safe_to_resume=1` 확인
- `recovery-report`에서 unresolved/recovery-required 및 `resume_blockers`를 함께 확인
- 재개 후 30~60분은 수동 모니터링 유지
- 거래소 API 응답 이상/부분 실패 시 `resume` 대신 pause 상태 유지 후 재검증
- `recover-order --client-order-id <id> --exchange-order-id <id> --yes`는 `RECOVERY_REQUIRED` 주문 복구 전용이며, 실행 후에도 `resume`은 별도로 수행해야 한다

## 6) Kill switch 운용 메모

- `KILL_SWITCH=true`: 신규 주문 차단용 비상 스위치
- `KILL_SWITCH_LIQUIDATE=true`: kill switch 동작 시 flatten 시도를 추가로 수행
- kill switch 해제 전 `health`/`recovery-report`/`reconcile`로 정합성 재확인


## 7) healthcheck / backup 운영 파라미터

- healthcheck 기본 임계치:
  - `HEALTH_MAX_CANDLE_AGE_SEC=180`
  - `HEALTH_MAX_ERROR_COUNT=3`
  - `HEALTH_MAX_RECONCILE_AGE_SEC=900`
  - `HEALTH_MAX_UNRESOLVED_ORDER_AGE_SEC=900`
- 백업 복구 검증(권장):

```bash
BACKUP_VERIFY_RESTORE=1 ./scripts/backup_sqlite.sh
python3 tools/verify_sqlite_restore.py backups/<backup_file>.sqlite
```

## 8) systemd env 파일 일관성 점검 (무인 전 필수)

- `bithumb-bot.service`는 `BITHUMB_ENV_FILE=/etc/bithumb-bot/bithumb-bot.live.env`를 사용한다.
- `bithumb-bot-healthcheck.service` / `bithumb-bot-backup.service`는 `EnvironmentFile=-/etc/bithumb-bot/bithumb-bot.env`를 사용한다.
- 따라서 무인 운용 전 `DB_PATH`/알림/임계치가 두 env 파일에서 일치하는지 운영자가 직접 점검해야 한다.
