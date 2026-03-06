# RUNBOOK (24/7 운영 초안)

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

## 3) 기본 점검

```bash
sudo systemctl restart bithumb-bot.service
sudo systemctl status bithumb-bot.service
sudo journalctl -u bithumb-bot.service -n 100 --no-pager

sudo systemctl list-timers | rg 'bithumb-bot-(healthcheck|backup)'
uv run python bot.py health
./scripts/backup_sqlite.sh
```

- `systemctl restart` 이후 서비스가 자동 실행 상태(`active (running)`)인지 확인.
- health 출력에서 `trading_enabled=True` 여부, `error_count` 및 `last_candle_age_sec` 확인.
- `backups/`에 백업 파일 생성 여부 확인.

## 4) 장애 대응 절차

### A. 재시작/프로세스 크래시

1. `sudo systemctl status bithumb-bot.service`로 재시작 루프 여부 확인.
2. `sudo journalctl -u bithumb-bot.service -n 200 --no-pager`로 직전 예외 확인.
3. 환경변수 변경 후 `sudo systemctl restart bithumb-bot.service`.
4. 3~5분 모니터링 후 healthcheck 알림 미발생 확인.

### B. 중복 주문 의심

1. `uv run python bot.py orders --limit 100`로 최근 주문 상태 확인.
2. 동일 시점/동일 방향의 order가 중복인지 확인.
3. live 모드면 거래소 체결 내역과 `fills` 비교.
4. 필요시 봇 일시 중지: `sudo systemctl stop bithumb-bot.service`.
5. 수동 정리 후 재기동: `sudo systemctl start bithumb-bot.service`.

### C. 잔고 불일치

1. `uv run python bot.py audit` 실행.
2. 불일치 시 `uv run python bot.py pnl --days 1` 및 `trades`/`fills` 대조.
3. live 모드면 브로커 잔고 API 기준으로 reconcile 수행.
4. 원인(수수료/슬리피지/부분체결 반영 누락) 확인 전 신규 주문 중단.

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

## 5) 알림 설정

하나 이상 설정하면 webhook 알림 사용, 미설정 시 콘솔 출력만 수행.

- Generic webhook: `NOTIFIER_WEBHOOK_URL`
- Slack incoming webhook: `SLACK_WEBHOOK_URL`
- Telegram bot: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`

권장:

- `NOTIFIER_ENABLED=true`
- `NOTIFIER_TIMEOUT_SEC=5`
- 비밀키/URL은 `/etc/bithumb-bot/bithumb-bot.env`에만 저장하고 로그에 출력 금지.

## 6) 백업 정책

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
