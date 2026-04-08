# bithumb-bot

媛꾨떒??SMA 湲곕컲 鍮쀬뜽 ?섏씠???ㅺ굅??遊뉗엯?덈떎.

## 鍮좊Ⅸ ?쒖옉

```bash
uv sync
uv run pytest -q
```

## CLI/?뷀듃由ы룷?명듃 (canonical)

- canonical CLI: `uv run bithumb-bot <command>`
  - `pyproject.toml`??`project.scripts`???깅줉??怨듭떇 ?뷀듃由ы룷?명듃?낅땲??
- ?명솚 ?뷀듃由ы룷?명듃(?숈씪 ?숈옉):
  - `uv run python -m bithumb_bot <command>`
  - `uv run python bot.py <command>`

## env 濡쒕뵫 洹쒖튃 (肄붾뱶 湲곗?)

- 湲곕낯?곸쑝濡??고??꾩? `.env`瑜?**?먮룞 濡쒕뵫?섏? ?딆뒿?덈떎**.
- env ?뚯씪 濡쒕뵫? `BITHUMB_ENV_FILE*` 怨꾩뿴??紐낆떆?덉쓣 ?뚮쭔 ?섑뻾?⑸땲??
  - `BITHUMB_ENV_FILE=/path/to/file.env` (理쒖슦??
  - `MODE=live`?대㈃ `BITHUMB_ENV_FILE_LIVE`
  - `MODE=paper`/`test`?대㈃ `BITHUMB_ENV_FILE_PAPER`
- healthcheck ?ㅽ겕由쏀듃??fail-fast ?뺤콉?쇰줈 **紐낆떆??env ?뚯씪???놁쑝硫??ㅽ뙣**?⑸땲??

濡쒖뺄?먯꽌 `.env`瑜??곕젮硫?紐낆떆?곸쑝濡?吏?뺥븯?몄슂.

```bash
BITHUMB_ENV_FILE=.env uv run bithumb-bot health
```

Runtime artifacts such as health/recovery reports and operator snapshots belong under env-injected runtime roots, not repo-relative paths like `./data`, `./backups`, or `./tmp`.

## ?먯＜ ?곕뒗 紐낅졊

```bash
uv run bithumb-bot sync
uv run bithumb-bot ticker
uv run bithumb-bot candles --limit 5
uv run bithumb-bot signal --short 7 --long 30
uv run bithumb-bot explain --short 7 --long 30
uv run bithumb-bot status
uv run bithumb-bot trades --limit 20
uv run bithumb-bot ops-report --limit 20
uv run bithumb-bot decision-telemetry --limit 200
uv run bithumb-bot cash-drift-report --recent-limit 5
uv run bithumb-bot experiment-report --sample-threshold 30 --top-n 3
uv run bithumb-bot run --short 7 --long 30
```

- ?댁쁺???꾨왂/?먯씡 寃利??덉감: `docs/OPERATOR_REPORTING.md`

## smoke/manual DB 寃利?寃쎈줈 ?뺤콉

- smoke/manual ?ㅽ뻾?먯꽌 ?앹꽦/蹂寃쎈릺??SQLite???댁쁺 嫄곕옒 ?먯옣 ?깃꺽(`data/<mode>/trades`)?쇰줈 痍④툒?⑸땲??
- ?곕씪???덊룷 ?대? ?곷?寃쎈줈(`./tmp`, `./data`, `./backups`) DB瑜??ъ슜?섏? 留먭퀬, **?덈?寃쎈줈 + ?덊룷 ?몃?** DB留??ъ슜?섏꽭??
- `tools/oms_smoke.py`??repo-local DB瑜?李⑤떒?⑸땲?? 湲곕낯 寃쎈줈??`DB_PATH` env?대ŉ, ?꾩슂 ??`--db-path`濡??덈?寃쎈줈瑜?二쇱엯?섏꽭??

?덉떆(?꾩떆 寃利?DB瑜??덊룷 ?몃? temp dir???앹꽦):

```bash
tmp_dir="$(mktemp -d)"
MODE=paper \
RUN_ROOT="$tmp_dir/run" DATA_ROOT="$tmp_dir/data" LOG_ROOT="$tmp_dir/logs" BACKUP_ROOT="$tmp_dir/backup" ENV_ROOT="$tmp_dir/env" \
DB_PATH="$tmp_dir/data/paper/trades/paper.sqlite" \
uv run bithumb-bot sync
MODE=paper DB_PATH="$tmp_dir/data/paper/trades/paper.sqlite" uv run python tools/oms_smoke.py
```

- 寃利????덊룷 ?ㅼ뿼 ?щ?瑜??먭??섎젮硫?

```bash
./scripts/check_repo_runtime_artifacts.sh
```

## 寃쎈줈 ?뺤콉 (PathManager 湲곗?)

- ???洹쒖튃 湲곗? 臾몄꽌:
  - `docs/storage-layout.md`
  - `docs/runtime-data-policy.md`
- 寃쎈줈??env(`ENV_ROOT`, `RUN_ROOT`, `DATA_ROOT`, `LOG_ROOT`, `BACKUP_ROOT`, `ARCHIVE_ROOT`)濡?二쇱엯?섍퀬, ?섏쐞 援ъ“(`run/<mode>`, `data/<mode>/*`, `logs/<mode>/*`, `backup/<mode>/*`)??肄붾뱶(PathManager)媛 媛뺤젣?⑸땲??
- `DB_PATH`, `RUN_LOCK_PATH`, `BACKUP_DIR`??**?먯쭊???명솚??override**濡??좎??⑸땲??
  - `DB_PATH` 誘몄꽕???? `DATA_ROOT/<mode>/trades/<mode>.sqlite`
  - `RUN_LOCK_PATH` 誘몄꽕???? `RUN_ROOT/<mode>/bithumb-bot.lock`
  - `BACKUP_DIR` 誘몄꽕???? `BACKUP_ROOT/<mode>/db`
- ?댁쁺 蹂댁“ ?ㅽ겕由쏀듃(`scripts/check_live_runtime.sh`, `scripts/collect_live_snapshot.sh`, `scripts/backup_sqlite.sh`)??PathManager 議고쉶(`python -m bithumb_bot.paths --kind ...`)瑜??ъ슜???숈씪 寃쎈줈 怨꾩빟???곕쫭?덈떎.
- ?댁쁺 ?곗텧臾?湲곕낯 ?꾩튂:
  - run lock / pid / runtime state: `RUN_ROOT/<mode>/`
  - DB: `DATA_ROOT/<mode>/trades/`
  - ops/strategy/fee/recovery report: `DATA_ROOT/<mode>/reports/<topic>/`
    - `ops_report_YYYY-MM-DD.json`
    - `strategy_validation_YYYY-MM-DD.json`
    - `fee_diagnostics_YYYY-MM-DD.json`
    - `recovery_report_YYYY-MM-DD.json`
  - trade ledger artifact(JSONL): `DATA_ROOT/<mode>/trades/<topic>/`
  - derived artifact(JSONL): `DATA_ROOT/<mode>/derived/<topic>/`
  - raw artifact(JSONL): `DATA_ROOT/<mode>/raw/<topic>/`
  - ?뚯씪 濡쒓렇(?꾩슂 ??: `LOG_ROOT/<mode>/<kind>/`
    - `kind ??{app, strategy, orders, fills, errors, audit}`
  - snapshot archive: `BACKUP_ROOT/<mode>/snapshots/`
  - DB backup: `BACKUP_ROOT/<mode>/db/`
- `MODE=live`?먯꽌????猷⑦듃 蹂?섎뱾???꾩닔?대ŉ, repo ?대? 寃쎈줈/?곷?寃쎈줈??fail-fast濡?李⑤떒?⑸땲??
- `MODE=paper`?먯꽌??濡쒖뺄 媛쒕컻 ?몄쓽瑜??꾪빐 ?곷?寃쎈줈 猷⑦듃瑜??덉슜?섎릺, ?댁쁺 諛고룷?먯꽌??live? ?숈씪?섍쾶 ?덈?寃쎈줈瑜?沅뚯옣?⑸땲??

## run lock ?숈옉

- `run` 紐낅졊? ?쒖옉 ??run lock???띾뱷?섎ŉ, ?대? ?ㅻⅨ run loop媛 ?ㅽ뻾 以묒씠硫?利됱떆 ?ㅽ뙣?⑸땲??
- lock 寃쎈줈??`RUN_LOCK_PATH`(誘몄꽕????`RUN_ROOT/<mode>/bithumb-bot.lock`)?낅땲??
- lock 異⑸룎 ???꾩옱 owner PID/host/?앹꽦?쒓컖/lock age ?뺣낫瑜??ы븿???먮윭瑜?異쒕젰?⑸땲??
- native Windows?먯꽌??`fcntl` 誘몄??먯쑝濡?run lock???숈옉?섏? ?딆쑝硫? ?먮윭 硫붿떆吏?濡?WSL/Linux?먯꽌 ?ㅽ뻾?댁빞 ?⑸땲??

## 二쇱슂 ?섍꼍 蹂??
?꾩옱 肄붾뱶?먯꽌 ?ㅼ젣濡??ъ슜?섎뒗 二쇱슂 ?듭뀡?낅땲??

- `MODE` (湲곕낯: `paper`)
- `MARKET` (湲곕낯: `KRW-BTC`, canonical)
- `PAIR` (legacy alias. `MARKET` 誘몄꽕???쒖뿉留??ъ슜; `MODE=live`?먯꽌??`KRW-BTC` canonical留??덉슜?섍퀬 `BTC_KRW`/`BTC`??嫄곕?)
- `INTERVAL` (湲곕낯: `1m`)
- `EVERY` (湲곕낯: `60`)
- `STRATEGY_NAME` (湲곕낯: `sma_with_filter`)
- `SMA_SHORT` (湲곕낯: `7`)
- `SMA_LONG` (湲곕낯: `30`)
- `COOLDOWN_MIN` (湲곕낯: `1`)
- `MIN_GAP` (湲곕낯: `0.0003`)
- `SMA_COST_EDGE_ENABLED` (湲곕낯: `true`, `sma_with_filter`??cost-edge 李⑤떒 on/off)
- `SMA_COST_EDGE_MIN_RATIO` (湲곕낯: `STRATEGY_MIN_EXPECTED_EDGE_RATIO` fallback, ?놁쑝硫?`0`)
- `DB_PATH` (?먯쭊???명솚 override. 誘몄꽕????`DATA_ROOT/<mode>/trades/<mode>.sqlite`)
- `LIVE_MIN_ORDER_QTY` (湲곕낯: `0`, 0?대㈃ 鍮꾪솢??
- `LIVE_ORDER_QTY_STEP` (湲곕낯: `0`, 0?대㈃ 鍮꾪솢??
- `LIVE_ORDER_MAX_QTY_DECIMALS` (湲곕낯: `0`, 0?대㈃ 鍮꾪솢??

> `ENTRY_MODE`, `advise` 而ㅻ㎤??媛숈? 怨쇨굅 ?듭뀡/紐낅졊? ?꾩옱 CLI?먯꽌 ?ъ슜?섏? ?딆뒿?덈떎.

?꾨왂 ?좏깮? ?꾨? ?섍꼍蹂??二쇱엯(`STRATEGY_NAME`) 湲곕컲?대ŉ, ?고???諛고룷 ?섍꼍(AWS EC2/ECS/Lambda ???먯꽌 ?뚯씪 寃쎈줈 ?섎뱶肄붾뵫 ?놁씠 ?숈씪?섍쾶 ?숈옉?⑸땲?? ?댁쁺 湲곕낯媛믪? 泥닿껐 鍮꾩슜/?몄씠利덈? 怨좊젮??`sma_with_filter`?대ŉ, 諛깊뀒?ㅽ듃/鍮꾧탳媛 ?꾩슂?섎㈃ `STRATEGY_NAME=sma_cross`濡?利됱떆 override?????덉뒿?덈떎(??뚮Ц??怨듬갚 ?낅젰???뺢퇋?붾릺???숈옉).

`sma_with_filter`??`cost_edge` ?꾪꽣???댁쁺 env濡?議곗젙 媛?ν빀?덈떎.
- `SMA_COST_EDGE_ENABLED=true`(湲곕낯): 湲곗〈泥섎읆 cost_edge 湲곗? 誘몃떖?대㈃ `BLOCKED_ENTRY ... cost_edge`濡?李⑤떒?⑸땲??
- `SMA_COST_EDGE_ENABLED=false`: cost_edge 李⑤떒留??고쉶?⑸땲???ㅻⅨ gap/volatility/overextended ?꾪꽣??洹몃?濡??좎?).
- `SMA_COST_EDGE_MIN_RATIO`(0 ?댁긽): 鍮꾩슜 諛붾떏(`LIVE_FEE_RATE_ESTIMATE`, `STRATEGY_ENTRY_SLIPPAGE_BPS`, `ENTRY_EDGE_BUFFER_RATIO`)怨??④퍡 鍮꾧탳?섎뒗 理쒖냼 湲곕? ?ｌ? ?섑븳媛믪엯?덈떎.

?덉떆(AWS 諛고룷 ?섍꼍蹂?섎쭔?쇰줈 ?꾨왂 ?꾪솚):

```bash
# ?댁쁺 湲곕낯(肄붾뱶 ?섏젙 ?놁쓬)
STRATEGY_NAME=sma_with_filter

# 鍮꾧탳/諛깊뀒?ㅽ듃 ?명솚 紐⑤뱶
STRATEGY_NAME=sma_cross
```

## Live 紐⑤뱶(?ㅺ굅??

- `MODE=live`濡??ㅽ뻾?섎㈃ paper? ?숈씪??`orders/fills/trades/portfolio` ?먯옣 ?ㅽ궎留덈? ?ъ슜?⑸땲??
- ?꾩옱 ?먯궛 議고쉶(`get_balance`)??private REST `/v1/accounts` **snapshot** 湲곕컲?낅땲?? MyAsset(WebSocket) 湲곕컲 ?먯궛 ?ㅽ듃由쇱? ?꾩옱 援ы쁽?섏뼱 ?덉? ?딆뒿?덈떎.
- `MODE=live`?먯꽌??`DB_PATH`瑜?諛섎뱶??紐낆떆?댁빞 ?섎ŉ, **諛섎뱶???덈?寃쎈줈**?ъ빞 ?⑸땲???곷?寃쎈줈 湲덉?).
- `MODE=live` preflight??paper/test ?깃꺽???쇳빀 ?ㅼ젙??嫄곕??⑸땲?? ?? 湲곕낯/怨듭쑀 DB 寃쎈줈, paper ?꾩슜 ??`START_CASH_KRW`, `BUY_FRACTION`, `FEE_RATE`, `PAPER_FEE_RATE`, `PAPER_FEE_RATE_ESTIMATE`, `SLIPPAGE_BPS`)媛 ?ㅼ젙??寃쎌슦, ?먮뒗 live 蹂댄샇媛?`MAX_ORDER_KRW`, `MAX_DAILY_LOSS_KRW`, `MAX_DAILY_ORDER_COUNT`, `MAX_ORDERBOOK_SPREAD_BPS`, `MAX_MARKET_SLIPPAGE_BPS`, `LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS`)???좏슚??媛?> 0, ?좏븳媛??쇰줈 ?ㅼ젙?섏? ?딆? 寃쎌슦 湲곕룞 ?꾩뿉 fail-fast濡?李⑤떒?⑸땲??
- `MODE=live`?먯꽌??notifier媛 諛섎뱶???쒖꽦/?ㅼ젙?섏뼱 ?덉뼱???⑸땲??(`NOTIFIER_WEBHOOK_URL` ?먮뒗 `SLACK_WEBHOOK_URL` ?먮뒗 `TELEGRAM_BOT_TOKEN`+`TELEGRAM_CHAT_ID`). 誘몄꽕????湲곕룞???ㅽ뙣?⑸땲??
- `LIVE_DRY_RUN=true`瑜?耳쒕㈃ **private write ?붿껌(二쇰Ц/痍⑥냼/?곹깭蹂寃?** ? 李⑤떒?섍퀬, **private read-only GET 吏꾨떒 ?붿껌(`/v1/accounts`, `/v1/orders/chance` ??** ? ?ㅼ젣 API ?몄텧???덉슜?⑸땲??
- `MODE=live`에서는 `BITHUMB_API_KEY`, `BITHUMB_API_SECRET`과 `LIVE_REAL_ORDER_ARMED=true`를 명시적으로 설정해야 합니다.
- live/paper/dryrun 怨듯넻?쇰줈 `client_order_id`??`{mode_token}_{intent_ts}_{side_token}_{suffix}` 洹쒖튃?쇰줈 ?앹꽦?섎ŉ, 嫄곕옒???쒖빟??留욊쾶 ??긽 36???댄븯瑜?蹂댁옣?⑸땲???? `live_1775367720000_buy_f70fd9a0`). ??媛믪? 濡쒖뺄 ?먯옣/蹂듦뎄 ?곌퀎???앸퀎?먮줈 ?좎??⑸땲??
- Bithumb `/v2/orders` payload 洹쒖튃: 臾몄꽌 ?꾨뱶(`market`, `side`, `volume`, `price`, `order_type`) 湲곗??쇰줈 ?꾩넚?⑸땲?? ?쒖옣媛 留ㅼ닔??`side=bid`, `order_type=price`, `price=<珥?二쇰Ц湲덉븸 KRW>`濡??꾩넚?섍퀬 `volume`? 蹂대궡吏 ?딆뒿?덈떎. ?쒖옣媛 留ㅻ룄??`side=ask`, `order_type=market`, `volume=<留ㅻ룄 ?섎웾>`???ъ슜?⑸땲??
- Bithumb private `GET /v1/orders` ?쒕챸 洹쒖튃: JWT `query_hash`??**?ㅼ젣 ?꾩넚?섎뒗 query string怨??꾩쟾???숈씪??臾몄옄??*(?뚮씪誘명꽣 ?쒖꽌/諛곗뿴 ?쒓린 ?ы븿)濡?怨꾩궛?댁빞 ?⑸땲?? 蹂??꾨줈?앺듃??諛곗뿴 ?뚮씪誘명꽣瑜?`uuids[]`, `client_order_ids[]` 諛섎났 ???뺤떇?쇰줈 吏곷젹?뷀븯硫? ?숈씪 臾몄옄?댁쓣 HTTP query?먮룄 洹몃?濡??ъ슜?⑸땲??
- Bithumb private 議고쉶 ?묐떟(`GET /v1/order`, `GET /v1/orders`)? 嫄곕옒???묐떟 ?쒖젏/?곹깭???곕씪 ?쇰? ?섏튂 ?꾨뱶(`fee`, `volume`)媛 ?꾨씫?섍굅??alias ?꾨뱶濡??대젮?????덉뒿?덈떎. 蹂??꾨줈?앺듃???앸퀎???곹깭 寃利앹? ?꾧꺽???좎??섎㈃?? ?꾨씫???섏튂 ?꾨뱶??蹂댁닔??fallback(0 ?먮뒗 ?좊룄 怨꾩궛)?쇰줈 泥섎━??false schema HALT瑜?以꾩엯?덈떎.
- `order_rules_autosync=FALLBACK` ?먮뒗 `health`/`ops-report`??order-rule snapshot fallback 寃쎄퀬?? 嫄곕옒?뚯쓽 `/v1/orders/chance` rule data瑜?吏곸젒 ?뺣낫?섏? 紐삵빐 濡쒖뺄 蹂댁닔 洹쒖튃???곌퀬 ?덈떎???살엯?덈떎. ??寃쎄퀬??"?ㅼ＜臾몄씠 ?덉쟾?섎떎"???뺤씤???꾨땲?? 臾몄꽌?붾맂 chance-derived rule source媛 ?꾩쭅 ?뚮났?섏? ?딆븯?ㅻ뒗 ?좏샇?낅땲?? live 紐⑤뱶?먯꽌??required rule source媛 `chance_doc`媛 ?꾨땲硫?preflight媛 fail-fast ?섏뼱???⑸땲??
- `/v2/orders` ?ъ쟾 寃利앹? ?ㅼ젣 ?꾩넚 吏곸쟾??留덉?留??덉쟾?μ튂?낅땲?? submit payload??`validate_order_submit_payload`? rule-source 寃?щ? ?듦낵?댁빞 ?섎ŉ, ?쒖옣媛 留ㅼ닔/留ㅻ룄??媛곴컖 `side=bid, order_type=price, price=<KRW>`? `side=ask, order_type=market, volume=<?섎웾>` ?뺥깭濡쒕쭔 ?덉슜?⑸땲?? live 寃쎈줈?먯꽌 rule source媛 local fallback??癒몃Т瑜대㈃, ?대? 二쇰Ц ?뱀씤?쇰줈 ?쎌? 留먭퀬 preflight 寃쎄퀬濡??쎌뼱???⑸땲??

### ?ㅼ＜臾?arming 諛⑸쾿

1. 癒쇱? `LIVE_DRY_RUN=true`濡?異⑸텇??寃利앺빀?덈떎.
2. ?ㅼ＜臾?吏곸쟾?먮쭔 ?꾨옒 媛믪쓣 ?④퍡 ?ㅼ젙?⑸땲??
   - `LIVE_DRY_RUN=false`
   - `LIVE_REAL_ORDER_ARMED=true`
3. ??以??섎굹?쇰룄 ?꾨씫?섎㈃ 湲곕룞 ??fail-fast濡?利됱떆 醫낅즺?⑸땲??

?덉떆(蹂댁닔???뚯븸 怨꾩젙 live dry-run + notifier):

```bash
MODE=live DATA_ROOT=/var/lib/bithumb-bot/data RUN_ROOT=/var/lib/bithumb-bot/run LOG_ROOT=/var/lib/bithumb-bot/logs BACKUP_ROOT=/var/lib/bithumb-bot/backup ENV_ROOT=/var/lib/bithumb-bot/env \
DB_PATH=/var/lib/bithumb-bot/data/live/trades/live.small.safe.sqlite LIVE_DRY_RUN=true LIVE_REAL_ORDER_ARMED=false \
MAX_ORDER_KRW=30000 MAX_DAILY_LOSS_KRW=20000 MAX_DAILY_ORDER_COUNT=6 \
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/xxx/yyy/zzz \
BITHUMB_API_KEY=... BITHUMB_API_SECRET=... \
uv run bithumb-bot run
```

?ㅼ＜臾??꾪솚(?댁쁺??紐낆떆 ?뱀씤 ?꾩뿉留?:

```bash
MODE=live DATA_ROOT=/var/lib/bithumb-bot/data RUN_ROOT=/var/lib/bithumb-bot/run LOG_ROOT=/var/lib/bithumb-bot/logs BACKUP_ROOT=/var/lib/bithumb-bot/backup ENV_ROOT=/var/lib/bithumb-bot/env \
DB_PATH=/var/lib/bithumb-bot/data/live/trades/live.small.safe.sqlite LIVE_DRY_RUN=false LIVE_REAL_ORDER_ARMED=true \
MAX_ORDER_KRW=30000 MAX_DAILY_LOSS_KRW=20000 MAX_DAILY_ORDER_COUNT=6 \
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/xxx/yyy/zzz \
BITHUMB_API_KEY=... BITHUMB_API_SECRET=... \
uv run bithumb-bot run
```

paper/live DB 遺꾨━ ?덉떆(PathManager 湲곕낯 寃쎈줈 ?ъ슜):

```bash
# paper 寃利?MODE=paper \
RUN_ROOT=/var/lib/bithumb-bot/run DATA_ROOT=/var/lib/bithumb-bot/data LOG_ROOT=/var/lib/bithumb-bot/logs BACKUP_ROOT=/var/lib/bithumb-bot/backup ENV_ROOT=/var/lib/bithumb-bot/env \
uv run bithumb-bot run

# live 寃利??댁쁺
MODE=live \
RUN_ROOT=/var/lib/bithumb-bot/run DATA_ROOT=/var/lib/bithumb-bot/data LOG_ROOT=/var/lib/bithumb-bot/logs BACKUP_ROOT=/var/lib/bithumb-bot/backup ENV_ROOT=/var/lib/bithumb-bot/env \
DB_PATH=/var/lib/bithumb-bot/data/live/trades/live.sqlite \
uv run bithumb-bot run
```
- ?덉쟾?μ튂: `MAX_ORDER_KRW`, `MAX_DAILY_LOSS_KRW`, `MAX_DAILY_ORDER_COUNT`, `KILL_SWITCH`.
- ?ъ떆?????붿쭊??`reconcile`???섑뻾?섏뿬 ?대┛ 二쇰Ц/泥닿껐/?ы듃?대━?ㅻ? ?숆린?뷀빀?덈떎.

### 蹂댁닔???쇱씠釉??꾨줈??(沅뚯옣: 1,000,000 KRW 怨꾩젙)

珥덇린 ?ㅺ굅?섎뒗 ?꾨옒泥섎읆 **?묎쾶 ?쒖옉**?섎뒗 寃껋쓣 沅뚯옣?⑸땲??

- `MAX_ORDER_KRW=30000` (二쇰Ц 1?뚮떦 ??3%)
- `MAX_DAILY_LOSS_KRW=20000` (???먯떎 ??2%?먯꽌 利됱떆 HALT(臾닿린??以묒?, ?먮룞 ?ш컻 ?놁쓬))
- `MAX_DAILY_ORDER_COUNT=6` (怨쇰ℓ留?諛⑹?)
- `KILL_SWITCH=false` (鍮꾩긽?쒖뿉留?true)
- `KILL_SWITCH_LIQUIDATE=false` (?됱떆 off. ?꾩슂 ??`true`濡??ㅼ젙?섎㈃ kill switch ?숈옉 ???ъ???flatten??異붽?濡??쒕룄?⑸땲?? live preflight ?ㅽ뙣 ?ъ쑀???꾨떃?덈떎.)
- ???먯떎 ?쒕룄 珥덇낵 ???붿쭊? ?좉퇋 二쇰Ц ?꾩뿉 嫄곕옒瑜?**HALT**?섍퀬 ?ㅽ뵂二쇰Ц 痍⑥냼 + ?ъ???flatten???쒕룄???? ?몄텧/誘명빐寃??곹깭媛 ?⑥쑝硫??댁쁺??蹂듦뎄/?ш컻 ?뱀씤???붽뎄?⑸땲??
- `LIVE_DRY_RUN=true`濡?理쒖냼 諛섎굹???댁긽 寃利???`false` ?꾪솚

## ?쇱씠釉??쒖옉 ??泥댄겕由ъ뒪??(Startup)

1. `BITHUMB_ENV_FILE`(?먮뒗 `BITHUMB_ENV_FILE_LIVE`)媛 媛由ы궎??env ?뚯씪???쇱씠釉??덉쟾媛믪씠 諛섏쁺?섏뿀?붿? ?뺤씤
2. `uv run bithumb-bot health`?먯꽌 `trading_enabled=True`, `error_count` ??쓬, `last_candle_age_sec` ?뺤긽 ?뺤씤
3. `uv run bithumb-bot recovery-report`?먯꽌 誘명빐寃?二쇰Ц/蹂듦뎄 ?꾩슂 嫄댁닔? ?ㅻ옒??誘명빐寃?二쇰Ц ?붿빟(top 5) ?뺤씤
4. 泥섏쓬 ?쇱씠釉??꾪솚 ??`MODE=live`, `LIVE_DRY_RUN=true`濡?湲곕룞 ??濡쒓렇/?뚮┝ ?뺤씤
5. API ?ㅻ? ?쒖꽦?뷀븯湲???`pause/resume/reconcile` 紐낅졊???뺤긽 ?숈옉?섎뒗吏 ?먭?
6. 실주문 전환(`LIVE_DRY_RUN=false`, `LIVE_REAL_ORDER_ARMED=true`) 직후 30~60분 수동 모니터링

## 鍮꾩긽 ?뺤? / ?쇱떆以묒? / 蹂듦뎄

```bash
# 利됱떆 ?좉퇋 嫄곕옒 以묒?
uv run bithumb-bot pause

# ?곹깭 ?먭?
uv run bithumb-bot recovery-report
uv run bithumb-bot health

# (live) ?먭꺽 ?ㅽ뵂 二쇰Ц ?쇨큵 痍⑥냼
uv run bithumb-bot cancel-open-orders

# ?뺥빀???먭?
uv run bithumb-bot reconcile

# 蹂댁닔???ш컻(臾몄젣 ?덉쑝硫??먮룞 嫄곕?)
uv run bithumb-bot resume
```

- 湲닿툒 ?쒖뿉??`pause`瑜?癒쇱? ?ㅽ뻾?섍퀬, ?먯씤 ?뚯븙 ??`resume --force`???쇳븯?몄슂.
- `KILL_SWITCH=true`??留덉?留??덉쟾?μ튂濡??ъ슜?섍퀬, ?댁젣 ??諛섎뱶??二쇰Ц/泥닿껐 ?뺥빀?깆쓣 ?ㅼ떆 ?뺤씤?섏꽭??

## ?щ옒?????ш컻 ???꾩닔 ?뺤씤

1. `journalctl -u bithumb-bot.service -n 200 --no-pager`濡?留덉?留??덉쇅/?ㅽ듃?뚰겕 ?ㅻ쪟 ?먯씤 ?뺤씤
2. `uv run bithumb-bot recovery-report`?먯꽌 `unresolved_orders`, `recovery_required_orders`媛 0?몄? ?뺤씤 (0???꾨땲硫??ㅻ옒??二쇰Ц ?붿빟 紐⑸줉?쇰줈 ?곗꽑 ???????뺤씤)
3. `uv run bithumb-bot reconcile` ?ㅽ뻾 ???ㅼ떆 `recovery-report` ?뺤씤
4. live 紐⑤뱶硫?嫄곕옒???ㅽ뵂 二쇰Ц/泥닿껐 ?댁뿭怨?濡쒖뺄 `orders/fills`媛 ?쇱튂?섎뒗吏 ?섑뵆 ?議?5. `uv run bithumb-bot health` ?뺤긽 ?뺤씤 ??`uv run bithumb-bot resume`

## 24/7 ?댁쁺(systemd + healthcheck + backup)

- systemd ?좊떅: `deploy/systemd/`
  - `bithumb-bot.service` (`Restart=always`)
  - `bithumb-bot-healthcheck.timer` (1遺?二쇨린)
  - `bithumb-bot-backup.timer` (6?쒓컙 二쇨린)
- ?댁쁺 ?덉감 臾몄꽌: `docs/RUNBOOK.md`
- ?쒗븳??臾댁씤 ?댁슜 泥댄겕由ъ뒪???붿빟): `docs/LIMITED_UNATTENDED_CHECKLIST.md`
- 諛깆뾽 ?ㅽ겕由쏀듃: `scripts/backup_sqlite.sh`
- ???좊떅(`bithumb-bot.service`, `bithumb-bot-healthcheck.service`, `bithumb-bot-backup.service`) 紐⑤몢 `BITHUMB_ENV_FILE=@BITHUMB_ENV_FILE_LIVE@`瑜??ъ슜?섎룄濡??쒗뵆由우씠 ?묒꽦?섏뼱 ?덉뒿?덈떎. ?ㅼ튂 ??`render_units.sh`濡??ㅼ젣 寃쎈줈瑜?移섑솚????諛고룷?섏꽭??
- `bithumb-bot-healthcheck.service`??鍮꾨??뷀삎 systemd PATH ?섏〈?깆쓣 ?쒓굅?섍린 ?꾪빐 `BITHUMB_UV_BIN`(湲곕낯媛? ?뚮뜑 ?쒖젏 `command -v uv` 寃곌낵, ?놁쑝硫?`uv`)???ъ슜?⑸땲??
- systemd ?ㅽ뻾 怨꾩젙? `BITHUMB_RUN_USER`濡?二쇱엯?????덉쑝硫? 湲곕낯媛믪? ?좊떅 ?뚮뜑留곸쓣 ?ㅽ뻾???ъ슜??`id -un`)?낅땲??
- `bithumb-bot.service` / `bithumb-bot-paper.service`??`PYTHONUNBUFFERED=1`怨?`python -u`瑜??④퍡 ?ъ슜??journald?먯꽌 `[RUN]`/`[SKIP]` 濡쒓렇媛 吏??踰꾪띁留??놁씠 諛붾줈 蹂댁씠?꾨줉 援ъ꽦?⑸땲??

鍮좊Ⅸ ?뺤씤:

```bash
sudo systemctl restart bithumb-bot.service
uv run bithumb-bot health
./scripts/backup_sqlite.sh
```

## ?ㅽ뻾 ?섍꼍 吏??踰붿쐞

- 沅뚯옣/吏?? Linux (?? Ubuntu, AWS EC2 Linux)
  - systemd ?댁쁺? Linux?먯꽌留??꾩젣?⑸땲??
- Windows:
  - native Windows??run lock(`fcntl`) 誘몄??먯쑝濡?`run` 猷⑦봽 ?댁쁺 ??곸씠 ?꾨떃?덈떎.
  - 媛쒕컻/?ㅽ뻾? WSL2(Linux ?ъ슜?먭났媛??먯꽌 ?섑뻾?섏꽭??
## Test Groups

- Fast regression set:
  - `uv run pytest -q -m fast_regression`
- Slow integration/live-like set:
  - `uv run pytest -q -m slow_integration`
- Prefer running the fast regression set first. Keep the slow set separate unless you are validating restart, recovery, or live-like execution paths.

