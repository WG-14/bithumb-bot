# Live Dry-Run Checklist

## 목적
실주문 전, live 모드에서 dry-run으로 systemd 24/7 운영이 안정적인지 확인한다.

## 시작 전 확인
- [ ] `.env.live`가 live DB/lock 경로를 사용한다
- [ ] `LIVE_DRY_RUN=true`
- [ ] `LIVE_REAL_ORDER_ARMED=false`
- [ ] notifier 설정이 정상이다
- [ ] `data/live.sqlite` 백업이 가능하다
- [ ] `bithumb-bot.service`가 정상 기동된다
- [ ] `bithumb-bot-healthcheck.timer`가 활성화되어 있다
- [ ] `bithumb-bot-backup.timer`가 활성화되어 있다

## 기동 확인
- [ ] `sudo systemctl status bithumb-bot.service`
- [ ] `sudo journalctl -u bithumb-bot.service -n 100 --no-pager`
- [ ] healthcheck 에러가 없다
- [ ] halt 상태가 없다
- [ ] 재시작 후 자동 복구된다

## dry-run 운영 중 확인
- [ ] 중복 실행이 없다
- [ ] run lock이 정상 동작한다
- [ ] reconcile 오류가 없다
- [ ] unresolved open order가 비정상적으로 쌓이지 않는다
- [ ] notifier가 정상 동작한다
- [ ] backup timer가 정상 수행된다

## 실주문 전환 전 최소 조건
- [ ] 일정 기간 연속 실행 중 치명 오류 없음
- [ ] systemd restart/reboot 후 정상 복귀
- [ ] healthcheck가 운영 중 정상 보고
- [ ] incident 대응 절차 문서화 완료
- [ ] rollback/restore 절차 준비 완료
