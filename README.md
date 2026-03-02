# bithumb-bot

간단한 SMA 기반 빗썸 페이퍼 트레이딩 봇입니다.

## 빠른 시작

```bash
uv sync
uv run python bot.py run --short 2 --long 5 --entry regime
```

## 자주 쓰는 명령

```bash
uv run python bot.py status
uv run python bot.py trades --limit 20
uv run python bot.py advise --short 2 --long 5 --entry cross
```

## HOLD가 계속 나올 때

- `cross` 모드는 SMA 교차 "순간"에만 BUY/SELL이 발생해서 HOLD가 자주 나옵니다.
- `regime` 모드는 `short > long` 상태에서 포지션이 없으면 BUY, `short < long` 상태에서 포지션이 있으면 SELL을 실행합니다.
- `MIN_GAP`이 크거나 `COOLDOWN_BARS`가 크면 HOLD 빈도가 더 올라갑니다.

`run` 로그에는 HOLD 이유가 함께 출력됩니다.

## Quickstart

```powershell
uv sync
copy .env.example .env   # or create .env manually (never commit secrets)
uv run pytest -q
uv run python bot.py run
