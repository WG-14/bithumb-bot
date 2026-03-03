# bithumb-bot

간단한 SMA 기반 빗썸 페이퍼 트레이딩 봇입니다.

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

> `ENTRY_MODE`, `advise` 커맨드 같은 과거 옵션/명령은 현재 CLI에서 사용하지 않습니다.
