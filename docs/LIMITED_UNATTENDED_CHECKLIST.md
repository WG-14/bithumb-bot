# Limited Unattended Live Ops Checklist (Bithumb BTC)

짧은 운영용 체크리스트입니다. 범위는 **Bithumb BTC 단일 전략, 제한적 무인 운용(수시 수동 점검 전제)** 입니다.

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

- `broker-diagnose`의 `overall_status=OK`
- `health`에 stale/error 이상 없음
- `recovery-report`에서 unresolved / recovery-required 정리 완료

## 3) API 키/권한 + notifier 체크

- [ ] Bithumb API 키: 조회 + 주문 권한 확인 (출금 권한 비활성 권장)
- [ ] notifier 필수 설정 확인 (`NOTIFIER_WEBHOOK_URL` 또는 `SLACK_WEBHOOK_URL` 또는 Telegram 조합)
- [ ] 알림 채널에 최근 health/recovery 알림이 정상 도착하는지 점검

## 4) 운영 중 즉시 제어 (문제 발생 시)

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

- `resume --force`는 마지막 수단으로만 사용

비상 노출 축소(필요 시):

```bash
uv run python bot.py flatten-position --dry-run
uv run python bot.py flatten-position
```

## 5) 재시작 후 검증 플로우 (restart verification)

```bash
uv run python bot.py restart-checklist
uv run python bot.py health
uv run python bot.py recovery-report
uv run python bot.py reconcile
uv run python bot.py recovery-report
uv run python bot.py resume
```

판정:

- `restart-checklist`의 `safe_to_resume=1` 확인
- 재개 후 30~60분은 수동 모니터링 유지

## 6) Kill switch 운용 메모

- `KILL_SWITCH=true`: 신규 주문 차단용 비상 스위치
- `KILL_SWITCH_LIQUIDATE=true`: 현재 미지원(설정 시 preflight 실패)
- kill switch 해제 전 `health`/`recovery-report`/`reconcile`로 정합성 재확인
