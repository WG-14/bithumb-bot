# RUNBOOK (Bithumb BTC ?뚯븸 ?ㅼ슫?? ?쒗븳??臾댁씤 ?댁슜)

> 踰붿쐞 怨좎젙: **Bithumb / BTC 留덉폆留?/ ?꾩옱 援ы쁽???⑥씪 ?꾨왂留?/ ?쒗븳??臾댁씤 ?댁슜(二쇨컙 ?섏떆 ?먭? ?꾩젣)**.
> 
> ??臾몄꽌??**?꾩쟾 24/7 ?먯쑉?댁슜** 媛?대뱶媛 ?꾨땲?? ?댁쁺?먭? ?섎（ 以??щ윭 李⑤? ?곹깭瑜??뺤씤?섎뒗 ?댁슜 紐⑤뜽??湲곗??쇰줈 ?쒕떎.

## ?댁쁺 紐⑤뜽 ??以??붿빟 (pilot reality)

- ??遊뉗? ?꾩옱 **"?쒗븳??臾댁씤 + 蹂댁닔??HALT"** 紐⑤뜽?대떎.
- `systemd` ?ъ떆?묒? ?먮룞?댁?留? ?꾪뿕/誘명빐寃??곹깭媛 媛먯??섎㈃ **?먮룞 ?ш컻 ???李⑤떒(HALT/Resume gate)** ???곗꽑?쒕떎.
- 利? "??긽 ?먮룞 蹂듦뎄?섎뒗 24/7 ?먯쑉 ?쒖뒪?????꾨땲??**?댁쁺???ъ젙??reconcile) + ?뱀씤 ?ш컻(resume)** 瑜??꾩젣濡???pilot ?④퀎??
- 嫄곕옒???묐떟 吏???꾨씫 泥닿껐 媛숈? 寃쎄퀎 ?щ????섍꼍留덈떎 ?ㅻ? ???덉쑝誘濡? **live?먯꽌??`recovery-report` 寃곌낵瑜?理쒖쥌 ?먮떒 湲곗?**?쇰줈 ?ъ슜?쒕떎.

## 0) ?댁슜 紐⑤뱶 援щ텇 (諛섎뱶??癒쇱? ?뺤씤)

?꾨옒 4媛吏瑜??쇰룞?섏? ?딅뒗??

- [ ] **paper**: ?쒕??덉씠???댁슜. ?ㅺ굅?섏냼 ?먭툑/二쇰Ц ?곹뼢 ?놁쓬.
- [ ] **live + dry-run**: ?쇱씠釉?寃쎈줈 ?먭? 紐⑤뱶. 嫄곕옒??議고쉶??媛?ν븯吏留??ㅼ＜臾몄? 湲덉? (`LIVE_DRY_RUN=true`).
- [ ] **live + armed**: ?ㅼ＜臾??덉슜 紐⑤뱶 (`LIVE_DRY_RUN=false`, `LIVE_REAL_ORDER_ARMED=true`).
- [ ] **live + not-armed**: `LIVE_DRY_RUN=false`?쇰룄 `LIVE_REAL_ORDER_ARMED!=true`硫?fail-fast濡?湲곕룞 ?ㅽ뙣?댁빞 ?뺤긽.

?댁쁺 ?쒖옉 ???좎뼵:

- [ ] ?ㅻ뒛 ?몄뀡 紐⑹쟻? `paper` / `live dry-run` / `live armed` 以??섎굹濡?紐낇솗???좏깮?덈떎.
- [ ] BTC ???щ낵/?ㅼ쨷?먯궛 ?댁슜? ?섏? ?딅뒗??
- [ ] ?⑥씪 ?꾨왂 ??而ㅼ뒪? ?ㅽ뿕 肄붾뱶???쇱씠釉뚯뿉 ?щ━吏 ?딅뒗??

## 1) 1,000,000 KRW ?뚯븸 怨꾩젙 蹂댁닔 ?꾨줈??(沅뚯옣)

?ㅺ굅??珥덇린媛믪? ?꾨옒泥섎읆 蹂댁닔?곸쑝濡??쒖옉?쒕떎.

- `MAX_ORDER_KRW=30000` (怨꾩젙????3%)
- `MAX_DAILY_LOSS_KRW=20000` (怨꾩젙????2% ?먯떎 ??利됱떆 HALT(臾닿린??以묒?, ?먮룞 ?ш컻 ?놁쓬))
- `MAX_DAILY_ORDER_COUNT=6` (怨쇰ℓ留??ㅼ옉???몄텧 異뺤냼)
- `KILL_SWITCH=false`, `KILL_SWITCH_LIQUIDATE=false` (?됱떆 off; ?꾩슂 ?쒖뿉留?鍮꾩긽 ?뺤?/泥?궛 ?덉감???곕씪 ?ъ슜)
- `LIVE_DRY_RUN=true`濡?癒쇱? ?댁쁺 寃쎈줈瑜?寃利앺븯怨? ?뺤씤 ??`false` ?꾪솚
- ???먯떎 ?쒕룄 珥덇낵 ???붿쭊? ?좉퇋 二쇰Ц ???④퀎?먯꽌 嫄곕옒瑜?**HALT**?섍퀬 ?ㅽ뵂二쇰Ц 痍⑥냼 + ?ъ????됲깂??flatten)瑜??쒕룄???? ?몄텧/誘명빐寃??곹깭媛 ?⑥쑝硫??댁쁺??蹂듦뎄/?ш컻 ?뱀씤???붽뎄?쒕떎.

> ?듭떖 ?먯튃: **二쇰Ц ?ш린蹂대떎 ?앹〈???곗꽑**. 珥덈컲 1~2二쇰뒗 ?섏씡蹂대떎 ?덉젙??寃利앹뿉 吏묒쨷.

## 2) 諛고룷 援ъ꽦

- `deploy/systemd/bithumb-bot.service`: 硫붿씤 ?몃젅?대뵫 猷⑦봽 (`Restart=always`).
- `deploy/systemd/bithumb-bot-healthcheck.timer`: 1遺꾨쭏???곹깭 ?먭?.
- `deploy/systemd/bithumb-bot-backup.timer`: 6?쒓컙留덈떎 SQLite 諛깆뾽.
- `scripts/healthcheck.py`: stale candle / ?ㅻ쪟 ?잛닔 / trading disabled 媛먯?.
- `scripts/backup_sqlite.sh`: sqlite `.backup` 湲곕컲 ?ㅻ깄??+ 蹂닿? ?뺤콉.

?뚮옯??踰붿쐞:

- ?댁쁺 ??? Linux (?? Ubuntu, AWS EC2 Linux)
- native Windows??`run` lock(`fcntl`) 誘몄??먯쑝濡??댁쁺 ????꾨떂
- Windows ?ъ슜?먮뒗 WSL2(Linux)?먯꽌 ?ㅽ뻾

## 3) ?ㅼ튂 諛??쒖꽦??
```bash
sudo mkdir -p /etc/bithumb-bot
sudo cp .env.example /etc/bithumb-bot/bithumb-bot.live.env

RENDER_DIR="$(mktemp -d)"
BITHUMB_BOT_ROOT="$(pwd)" \
BITHUMB_UV_BIN="$(command -v uv)" \
BITHUMB_RUN_USER="$(id -un)" \
./deploy/systemd/render_units.sh "${RENDER_DIR}"
sudo cp "${RENDER_DIR}"/bithumb-bot.service /etc/systemd/system/
sudo cp "${RENDER_DIR}"/bithumb-bot-healthcheck.service /etc/systemd/system/
sudo cp "${RENDER_DIR}"/bithumb-bot-healthcheck.timer /etc/systemd/system/
sudo cp "${RENDER_DIR}"/bithumb-bot-backup.service /etc/systemd/system/
sudo cp "${RENDER_DIR}"/bithumb-bot-backup.timer /etc/systemd/system/

sudo systemctl daemon-reload
sudo systemctl enable --now bithumb-bot.service
sudo systemctl enable --now bithumb-bot-healthcheck.timer
sudo systemctl enable --now bithumb-bot-backup.timer
```

- ?댁쁺 ??諛섎뱶??3媛??좊떅??env/DB ?쇨??깆쓣 ?먭??쒕떎.
  - `bithumb-bot.service`: `Environment=BITHUMB_ENV_FILE=/etc/bithumb-bot/bithumb-bot.live.env`
  - `bithumb-bot-healthcheck.service`, `bithumb-bot-backup.service`: `Environment=BITHUMB_ENV_FILE=/etc/bithumb-bot/bithumb-bot.live.env`
  - ???좊떅??媛숈? env ?뚯씪??蹂대?濡?`DB_PATH`, notifier, ?꾧퀎移섎? ?⑥씪 ?뚯씪 湲곗??쇰줈 愿由?
- `bithumb-bot.service` / `bithumb-bot-paper.service`??`PYTHONUNBUFFERED=1` + `python -u` + `@BITHUMB_UV_BIN@` 寃쎈줈瑜??ъ슜??systemd/journald ?섍꼍?먯꽌???곕（??濡쒓렇媛 利됱떆 異쒕젰?섎룄濡??좎??쒕떎.
- `bithumb-bot-healthcheck.service`??`User=@BITHUMB_RUN_USER@`, `WorkingDirectory=@BITHUMB_BOT_ROOT@`, `ExecStart=@BITHUMB_UV_BIN@ run python @BITHUMB_BOT_ROOT@/scripts/healthcheck.py` ?쒗뵆由우쑝濡??뚮뜑留곹븳??
- healthcheck??fail-fast ?뺤콉?대떎.
  - `BITHUMB_ENV_FILE`??鍮꾩뼱 ?덇굅???뚯씪???놁쑝硫?利됱떆 ?ㅽ뙣
  - env ?뚯씪 ??`DB_PATH`媛 鍮꾩뼱 ?덉뼱???ㅽ뙣(湲곕낯 DB ?먮룞 ?泥?湲덉?)

## 4) ?꾨━?쇱씠釉??덉쟾吏꾩엯) 泥댄겕由ъ뒪??
?꾨옒??**?ㅼ＜臾?吏꾩엯 ??*?먮쭔 ?섑뻾?섎뒗 泥댄겕由ъ뒪?몃떎. ?섎굹?쇰룄 ?ㅽ뙣?섎㈃ live armed濡??섏뼱媛吏 ?딅뒗??

### A. ?섍꼍/沅뚰븳/由ъ뒪???ㅼ젙 ?뺤씤

1. `MODE`媛 ?섎룄??紐⑤뱶?몄? ?뺤씤 (`paper`/`live` ?쇰룞 湲덉?)
2. `MODE=live`?쇰㈃ ?ㅼ쓬 媛믪씠 ?섎룄?濡??ㅼ젙?섏뿀?붿? ?ы솗??   - `MAX_ORDER_KRW > 0`
   - `MAX_DAILY_LOSS_KRW > 0`
   - `MAX_DAILY_ORDER_COUNT > 0`
   - `MAX_ORDERBOOK_SPREAD_BPS > 0` (?좏븳媛?
   - `MAX_MARKET_SLIPPAGE_BPS > 0` (?좏븳媛?
   - `LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS > 0` (?좏븳媛?
3. `MODE=live` 湲곕낯 吏꾩엯? `LIVE_DRY_RUN=true`濡??쒖옉
4. ?ㅼ＜臾??꾪솚 吏곸쟾?먮쭔 `LIVE_DRY_RUN=false` + `LIVE_REAL_ORDER_ARMED=true` ?숈떆 ?ㅼ젙
5. `KILL_SWITCH=false` ?뺤씤 (鍮꾩긽 ?쒖뿉留?true)
6. `KILL_SWITCH_LIQUIDATE`???꾩슂 ??鍮꾩긽 flatten ?쒕룄?⑹쑝濡쒕쭔 ?ъ슜
7. `.env.example` 蹂듭궗蹂몄쓣 洹몃?濡??곗? 留먭퀬 live ?꾩닔媛믪쓣 紐낆떆?곸쑝濡???뼱?대떎
   - 湲곕낯/怨듭쑀 DB 寃쎈줈(`data/bithumb_1m.sqlite`) 湲덉?
   - paper ?꾩슜 ??`START_CASH_KRW`, `BUY_FRACTION`, `FEE_RATE`, `PAPER_FEE_RATE`, `PAPER_FEE_RATE_ESTIMATE`, `SLIPPAGE_BPS`)??live?먯꽌 unset
8. API ??沅뚰븳 ?뺤씤 (?섎룞 ?먭?)
   - 議고쉶 + 二쇰Ц(?꾨Ъ) 沅뚰븳???덈뒗吏 ?뺤씤
   - 異쒓툑 沅뚰븳? 鍮꾪솢?깊솕 沅뚯옣
   - API ?ㅻ뒗 env ?뚯씪?먮쭔 ??ν븯怨?吏곸쟾 二쇱엯 ?먯튃 ?좎?
   - IP whitelist(?ъ슜 ?? ?ы븿 沅뚰븳 ?ㅼ퐫?꾨뒗 肄붾뱶媛 ?먮룞 寃利앺븯吏 ?딆쑝誘濡??댁쁺?먭? 吏곸젒 ?뺤씤
9. DB 遺꾨━ ?뺤씤
   - `paper`? `live`???쒕줈 ?ㅻⅨ `DB_PATH` ?ъ슜
   - `MODE=live`?먯꽌 湲곕낯 DB 寃쎈줈 ?ъ슜 湲덉? 洹쒖튃 以??
### B. ?꾨━?쇱씠釉?紐낅졊 ?쒖꽌 (怨좎젙)

?꾨옒 ?쒖꽌瑜?**洹몃?濡?* ?ㅽ뻾?쒕떎.

```bash
uv run bithumb-bot broker-diagnose
uv run bithumb-bot health
uv run bithumb-bot recovery-report
uv run bithumb-bot reconcile
uv run bithumb-bot recovery-report
```

?먯젙:

- `broker-diagnose`媛 `overall=PASS`媛 ?꾨땲硫??ㅼ＜臾?湲덉? (`overall=WARN/FAIL` 紐⑤몢 蹂대쪟)
- `health`?먯꽌 stale/error ?댁긽???덉쑝硫??먯씤 ?댁냼 ??吏꾪뻾 湲덉?
- `recovery-report`?먯꽌 unresolved/recovery-required媛 ?⑥븘 ?덉쑝硫?`reconcile` ???ы솗??
### C. ?쒕퉬??湲곕룞/濡쒓렇 ?뺤씤

```bash
sudo systemctl restart bithumb-bot.service
sudo systemctl status bithumb-bot.service
sudo journalctl -u bithumb-bot.service -n 100 --no-pager

uv run bithumb-bot health
uv run bithumb-bot recovery-report
```

- ?쒕퉬?ㅺ? `active (running)`?몄? ?뺤씤.
- `health`?먯꽌 `last_candle_age_sec`, `error_count`, `trading_enabled` ?뺤씤.
- `recovery-report`?먯꽌 unresolved/recovery-required 嫄댁닔? ?ㅻ옒??誘명빐寃?二쇰Ц ?붿빟(top 5) ?뺤씤.

## 5) 湲곕낯 ?먭? (?쇱긽 ?댁슜)

```bash
sudo systemctl list-timers | rg 'bithumb-bot-(healthcheck|backup)'
./scripts/backup_sqlite.sh
```

- timer媛 ?뺤긽 ?깅줉/?ㅽ뻾?섎뒗吏 ?뺤씤.
- `backups/` ?뚯씪 ?앹꽦 ?щ? ?뺤씤.

## 5-1) 釉뚮줈而??쎄린 ?꾩슜 吏꾨떒 (`broker-diagnose`)

?ㅼ＜臾????μ븷 議곗궗 ??**二쇰Ц ?놁씠** 嫄곕옒???곕룞 ?곹깭瑜?鍮좊Ⅴ寃??먭??쒕떎.

```bash
uv run bithumb-bot broker-diagnose
```

異쒕젰 ?붿빟 ??ぉ:

- ?ㅻ뜑: `[BROKER-READINESS]`, `pair=<PAIR>`
- ?붿빟: `summary: pass=<N> warn=<N> fail=<N> overall=PASS|WARN|FAIL`
- ?곸꽭: `- [PASS|WARN|FAIL] <check name>: <detail>`

?댁쁺 媛?대뱶:

- `overall=PASS`: ?쇱씠釉????먭? ?듦낵(?ㅼ쓬 ?④퀎 吏꾪뻾 媛??
- `overall=WARN`: 鍮꾩튂紐?寃쎄퀬 議댁옱(?먯씤 ?뺤씤 ??吏꾪뻾 ?щ? ?먮떒)
- `overall=FAIL`: ?듭떖 議고쉶 ?ㅽ뙣(鍮꾩젙??. ?먯씤 ?댁냼 ???ш컻/?ㅼ＜臾?湲덉?

二쇱쓽:

- `MODE=live`?먯꽌留??숈옉?쒕떎. 洹???紐⑤뱶?먯꽌???ㅽ뙣濡?醫낅즺?쒕떎.
- ??紐낅졊? 二쇰Ц ?앹꽦/痍⑥냼瑜??몄텧?섏? ?딅뒗 ?쎄린 ?꾩슜 吏꾨떒?대떎.

## 6) ?댁쁺??利됱떆 ?쒖뼱 泥댄겕由ъ뒪??(pause/resume/cancel)

臾몄젣 吏뺥썑(?ㅻ쪟 湲됱쬆, 泥닿껐/?붽퀬 遺덉씪移??섏떖, ?ㅽ듃?뚰겕 遺덉븞?? ???꾨옒 3媛?紐낅졊???곗꽑 ?ъ슜?쒕떎.

### A. 利됱떆 ?쇱떆以묒?

```bash
uv run bithumb-bot pause
```

- ?좉퇋 二쇰Ц 李⑤떒??理쒖슦??
- pause 吏곹썑 `health` + `recovery-report` + 理쒓렐 濡쒓렇瑜?蹂몃떎.

### B. ?ㅽ뵂 二쇰Ц ?뺣━

```bash
uv run bithumb-bot cancel-open-orders
```

- live 紐⑤뱶 ?먭꺽 誘몄껜寃?二쇰Ц???뺣━?쒕떎.
- ?ㅽ뻾 ??`reconcile` + `recovery-report`濡??뺥빀???ы솗??

### C. ?ш컻

```bash
uv run bithumb-bot resume
```

- blocker媛 ?⑥븘 ?덉쑝硫??ш컻?섏? ?딅뒗??
- `resume --force`??留덉?留??섎떒?쇰줈留??ъ슜?쒕떎.

## 7) 鍮꾩긽 ?뺤? / ?쇱떆以묒? / 蹂듦뎄 泥댄겕由ъ뒪??
### A. 利됱떆 由ъ뒪??李⑤떒 (Emergency stop)

```bash
# 1) ?좉퇋 嫄곕옒 利됱떆 以묒?
uv run bithumb-bot pause

# 2) (?좏깮) ?섍꼍?먯꽌 kill switch ?쒖꽦?????쒕퉬???ъ떆??# KILL_SWITCH=true
# sudo systemctl restart bithumb-bot.service
```

- ?먯씤 ?뺤씤 ?꾩뿉??`resume --force`瑜??ъ슜?섏? ?딅뒗??
- ?댁쁺???뱀씤 ?놁씠 ?ㅼ＜臾??ш컻 湲덉?.

### B. ?곹깭 ?뚯븙 (Pause ?곹깭?먯꽌)

```bash
uv run bithumb-bot health
uv run bithumb-bot recovery-report
sudo journalctl -u bithumb-bot.service -n 200 --no-pager
```

?뺤씤 ?ъ씤??
- 理쒓렐 ?ㅻ쪟媛 API/?ㅽ듃?뚰겕/?몄쬆 以?臾댁뾿?몄?
- 誘명빐寃?二쇰Ц(`unresolved_orders`) 議댁옱 ?щ?
- 蹂듦뎄 ?꾩슂 二쇰Ц(`recovery_required_orders`) 議댁옱 ?щ?
- ?ㅻ옒??二쇰Ц ?붿빟?먯꽌 `client_order_id`, `exchange_order_id`, `last_error`瑜??곗꽑 ?뺤씤??蹂듦뎄 ?곗꽑?쒖쐞 寃곗젙

### C. 蹂듦뎄 ?≪뀡 (?꾩슂 ???쒖꽌?濡?

```bash
# live 紐⑤뱶?먯꽌 ?먭꺽 誘몄껜寃??쇨큵 痍⑥냼
uv run bithumb-bot cancel-open-orders

# 嫄곕옒??濡쒖뺄 ?먯옣 ?뺥빀???먭?
uv run bithumb-bot reconcile

# 蹂듦뎄 ?곹깭 ?ы솗??uv run bithumb-bot recovery-report
```

### D. ?ш컻 (Recovery / Resume)

```bash
# 蹂댁닔???ш컻 (?댁긽 ?곹깭媛 ?덉쑝硫??먮룞 嫄곕?)
uv run bithumb-bot resume

# 留덉?留??섎떒: ?댁쁺??梨낆엫 ??媛뺤젣 ?ш컻
uv run bithumb-bot resume --force
```

由ъ뒪???ъ쑀(`KILL_SWITCH`, `DAILY_LOSS_LIMIT`, `POSITION_LOSS_LIMIT`)濡?HALT??寃쎌슦 異붽? 洹쒖튃:

- ?ъ????ㅽ뵂?ㅻ뜑 ???몄텧(exposure)???⑥븘 ?덉쑝硫?`resume`? 嫄곕??쒕떎.
- ?붿쭊? ?ㅽ뵂二쇰Ц 痍⑥냼? flatten???쒕룄?섏?留? ?ㅽ뙣/誘명빐寃????댁쁺?먭? 癒쇱? ?몄텧???섎룞 ?댁냼?댁빞 ?쒕떎.
- ?댁냼 ??`recovery-report`? `health`瑜??ㅼ떆 ?뺤씤?섍퀬 `resume`???ㅽ뻾?쒕떎.

`recover-order`??`RECOVERY_REQUIRED` ?곹깭 二쇰Ц?먮쭔 ?곸슜?섎ŉ, ?꾨즺 ?꾩뿉??嫄곕옒???먮룞 ?ш컻?섏? ?딅뒗??


?덉떆 (`uv run bithumb-bot recovery-report`):

```text
[P2] resume_eligibility
  resume_allowed=0
  can_resume=false
  blockers=STARTUP_SAFETY_GATE_BLOCKED, HALT_RISK_OPEN_POSITION
  force_resume_allowed=0
```

## 8) tiny-size ?ㅼ＜臾??ㅻえ???뚯뒪??(armed-live 吏곹썑 1??

?ㅼ＜臾??꾪솚 吏곹썑?먮뒗 ?꾨옒瑜?1???섑뻾??二쇰Ц-泥닿껐-湲곕줉 猷⑦봽瑜??뚯븸?쇰줈 寃利앺븳??

1. 二쇰Ц ?쒕룄瑜??쇱떆?곸쑝濡?媛???묒? ?덉쟾 媛믪쑝濡??좎? (`MAX_ORDER_KRW` 理쒖냼)
2. `MODE=live`, `LIVE_DRY_RUN=false`, `LIVE_REAL_ORDER_ARMED=true` ?뺤씤
3. 1??二쇰Ц/泥닿껐 諛쒖깮??紐⑤땲?곕쭅 (二쇰Ц 怨쇰떎 ?좊룄 湲덉?)
4. 利됱떆 ?ㅼ쓬 ?뺤씤
   - `uv run bithumb-bot health`
   - `uv run bithumb-bot recovery-report`
   - `uv run bithumb-bot reconcile`
5. `orders/fills/trades`??1???ъ씠?댁씠 ?뺤긽 湲곕줉?섎㈃ ?ㅻえ???듦낵
6. ?댁긽 吏뺥썑 ??`pause` -> `cancel-open-orders` -> ?먯씤 遺꾩꽍 ???ъ쭊??
## 9) ?щ옒???ъ떆?????ъ젙??寃利?(restart-and-reconcile)

?щ옒??媛뺤젣 ?ъ떆???댄썑?먮뒗 ?꾨옒瑜?紐⑤몢 ?뺤씤?섍린 ???ш컻?섏? ?딅뒗??

1. `journalctl`濡?留덉?留??덉쇅 ?먯씤???댁냼?섏뿀?붿? ?뺤씤
2. `uv run bithumb-bot recovery-report`?먯꽌 ?꾨옒媛 0?몄? ?뺤씤
   - `unresolved_orders`
   - `recovery_required_orders`
3. `uv run bithumb-bot reconcile` ???ㅼ떆 `recovery-report` ?ㅽ뻾
4. live 紐⑤뱶硫?嫄곕옒???ㅽ뵂 二쇰Ц/泥닿껐怨?濡쒖뺄 `orders/fills/trades` ?섑뵆 ?議?5. `uv run bithumb-bot health`?먯꽌 stale/error ?댁긽 ?놁쓬 ?뺤씤
6. `uv run bithumb-bot resume`濡??ш컻 ??30~60遺?紐⑤땲?곕쭅

### ?ъ떆??蹂듦뎄 ?쒖? ?뚮줈??(?댁쁺?먯슜 怨좎젙 ?덉감)

?꾨옒 ?쒖꽌瑜?湲곕낯媛믪쑝濡??ъ슜?쒕떎.

```bash
# 0) (?꾩슂 ?? 利됱떆 ?좉퇋 二쇰Ц 李⑤떒
uv run bithumb-bot pause

# 1) ?곹깭 ?뺤씤
uv run bithumb-bot health
uv run bithumb-bot recovery-report

# 2) ?뺥빀??蹂듦뎄
uv run bithumb-bot reconcile
uv run bithumb-bot recovery-report

# 3) 誘몄껜寃??몄텧???⑥쑝硫??뺣━ ???ш?利?uv run bithumb-bot cancel-open-orders
uv run bithumb-bot reconcile
uv run bithumb-bot recovery-report

# 4) blocker ?댁냼 ?쒖뿉留??ш컻
uv run bithumb-bot resume
```

?댁쁺 洹쒖튃:

- `resume`??嫄곕??섎㈃ 癒쇱? blocker瑜??댁냼?쒕떎 (`resume --force` ?곸떆 ?ъ슜 湲덉?).
- ?ш컻 ?먯젙? ?⑥닚 `unresolved_count`媛 ?꾨땲??`resume_blockers`(?? `STARTUP_SAFETY_GATE_BLOCKED`, `LAST_RECONCILE_FAILED`, `HALT_RISK_OPEN_POSITION`) 湲곕컲?대떎.
- 由ъ뒪???ъ쑀 HALT(`KILL_SWITCH`, `DAILY_LOSS_LIMIT`, `POSITION_LOSS_LIMIT`)?먯꽌???몄텧(?ъ????ㅽ뵂?ㅻ뜑) ?뺣━ ???ш컻媛 ?쒗븳?????덈떎.
- 媛뺤젣 ?ш컻(`resume --force`)??議곗궗/?뱀씤 濡쒓렇瑜??④릿 寃쎌슦?먮쭔 ?덉쇅?곸쑝濡??ъ슜?쒕떎.

## 10) ?μ븷 ????덉감 (?좏삎蹂?

### A. ?ъ떆???꾨줈?몄뒪 ?щ옒??
1. `sudo systemctl status bithumb-bot.service`濡??ъ떆??猷⑦봽 ?щ? ?뺤씤.
2. `sudo journalctl -u bithumb-bot.service -n 200 --no-pager`濡?吏곸쟾 ?덉쇅 ?뺤씤.
3. ?섍꼍蹂???ㅼ젙 ?섏젙 ??`sudo systemctl restart bithumb-bot.service`.
4. 3~5遺?紐⑤땲?곕쭅 ??healthcheck ?뚮┝ 誘몃컻???뺤씤.

### B. 以묐났 二쇰Ц ?섏떖

1. `uv run bithumb-bot orders --limit 100`濡?理쒓렐 二쇰Ц ?곹깭 ?뺤씤.
2. ?숈씪 ?쒖젏/?숈씪 諛⑺뼢??order媛 以묐났?몄? ?뺤씤.
3. live 紐⑤뱶硫?嫄곕옒??泥닿껐 ?댁뿭怨?`fills` 鍮꾧탳.
4. ?꾩슂??遊??쇱떆 以묒?: `uv run bithumb-bot pause`.
5. ?섎룞 ?뺣━ ???ш린???ш컻.

### C. ?붽퀬 遺덉씪移?
1. `uv run bithumb-bot audit` ?ㅽ뻾.
2. 遺덉씪移???`uv run bithumb-bot pnl --days 1` 諛?`trades`/`fills` ?議?
3. live 紐⑤뱶硫?釉뚮줈而??붽퀬 API 湲곗??쇰줈 reconcile ?섑뻾.
4. ?먯씤 ?뺤씤 ???좉퇋 二쇰Ц 以묐떒.

### D. ?곗씠???꾨씫 (罹붾뱾 stale / sync ?ㅽ뙣)

1. `uv run bithumb-bot health` ?뺤씤 (`last_candle_age_sec`).
2. `journalctl`?먯꽌 `sync failed`, `stale candle` 濡쒓렇 ?뺤씤.
3. ?ㅽ듃?뚰겕/API ?곹깭 ?뺤씤 ??`sudo systemctl restart bithumb-bot.service`.
4. ?щ컻 ??`EVERY`, `INTERVAL`, rate limit ?ㅼ젙 ?꾪솕.

### E. ?덉씠??由щ컠 ???
1. ?먮윭 濡쒓렇?먯꽌 HTTP 429/嫄곕옒???먮윭肄붾뱶 ?뺤씤.
2. `EVERY` 利앷?, ?ъ떆???몄텧 鍮덈룄 ?꾪솕.
3. healthcheck ?뚮┝ 鍮덈룄媛 ?믪쑝硫??꾧퀎移?`HEALTH_MAX_ERROR_COUNT`) 議곗젙.
4. 蹂듦뎄 ??10~15遺꾧컙 二쇰Ц/泥닿껐/罹붾뱾 ?먮쫫 ?먭?.

## 11) ?뚮┝ ?ㅼ젙

?섎굹 ?댁긽 ?ㅼ젙?섎㈃ webhook ?뚮┝ ?ъ슜, 誘몄꽕????肄섏넄 異쒕젰留??섑뻾.

- Generic webhook: `NOTIFIER_WEBHOOK_URL`
- Slack incoming webhook: `SLACK_WEBHOOK_URL`
- Telegram bot: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`

沅뚯옣:

- `NOTIFIER_ENABLED=true`
- `NOTIFIER_TIMEOUT_SEC=5`
- 鍮꾨???URL? env ?뚯씪?먮쭔 ??ν븯怨?濡쒓렇??異쒕젰 湲덉?.

## 12) 諛깆뾽 ?뺤콉

- 湲곕낯 寃쎈줈: `/var/lib/bithumb-bot/backup/<mode>/db/`
- 湲곕낯 蹂닿?: 7?? 理쒕? 30媛?- ?섍꼍蹂??
  - `BACKUP_DIR`
  - `BACKUP_RETENTION_DAYS`
  - `BACKUP_RETENTION_COUNT`
  - `BACKUP_VERIFY_RESTORE=1` (諛깆뾽 吏곹썑 `tools/verify_sqlite_restore.py`濡?蹂듦뎄 ?쎄린 寃利?

蹂듦뎄 ?덉떆:

```bash
sqlite3 /var/lib/bithumb-bot/data/live/trades/live.sqlite ".restore /var/lib/bithumb-bot/backup/live/db/live.sqlite.20260101_120000.sqlite"

# 諛깆뾽 ?뚯씪 蹂듦뎄 寃利?沅뚯옣)
python3 tools/verify_sqlite_restore.py /var/lib/bithumb-bot/backup/live/db/live.sqlite.20260101_120000.sqlite
```

## 13) Live 紐⑤뱶 ?ъ쟾 ?먭? (fail-fast)

`MODE=live`濡??쒖옉?섎㈃ ?고????쒖옉 ?꾩뿉 ?꾨옒 ??ぉ??媛뺤젣 寃利앺븳?? ?섎굹?쇰룄 ?꾨씫?섎㈃ 利됱떆 醫낅즺?쒕떎.

- `MAX_ORDER_KRW > 0`
- `MAX_DAILY_LOSS_KRW > 0`
- `MAX_DAILY_ORDER_COUNT > 0`
- `DB_PATH`??`MODE=live`?먯꽌 諛섎뱶??紐낆떆?댁빞 ?섎ŉ, ?곷?寃쎈줈 ?ъ슜 湲덉?(?덈?寃쎈줈 ?꾩닔)
- live preflight??paper/test ?깃꺽 ?쇳빀 ?ㅼ젙??李⑤떒?쒕떎(?? `START_CASH_KRW`, `BUY_FRACTION`, `FEE_RATE`, `PAPER_FEE_RATE`, `PAPER_FEE_RATE_ESTIMATE`, `SLIPPAGE_BPS`媛 ?ㅼ젙??寃쎌슦 嫄곕?)
- `MAX_ORDERBOOK_SPREAD_BPS`, `MAX_MARKET_SLIPPAGE_BPS`, `LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS`??live?먯꽌 `>0` ?좏븳媛??꾩닔
- `MODE=live`에서는 `BITHUMB_API_KEY`, `BITHUMB_API_SECRET` 필수
- `MODE=live`에서는 `LIVE_REAL_ORDER_ARMED=true`를 명시해야 실주문 허용
- `/v1/accounts` preflight?먯꽌 quote ?듯솕 row(?? KRW)????긽 ?꾩닔?대ŉ, `LIVE_DRY_RUN=true` + `LIVE_REAL_ORDER_ARMED=false` 議고빀?먯꽌??base ?듯솕 row ?꾨씫??0 蹂댁쑀(臾댄룷吏???쒖옉)濡??댁꽍???듦낵 媛??- ?ㅼ＜臾?寃쎈줈(`LIVE_DRY_RUN=false` + `LIVE_REAL_ORDER_ARMED=true`)?먯꽌??base ?듯솕 row ?꾨씫???덉슜?섏? ?딆쑝硫? 利됱떆 fail-fast 李⑤떒?쒕떎.
- ?댁쁺??吏꾨떒(`broker-diagnose`, `health`, `ops-report`)?먯꽌??`/v1/accounts` 愿???곹깭瑜?`execution_mode`, `quote_currency`, `base_currency`, `base_currency_missing_policy`, `preflight_outcome` ?꾨뱶濡??④퍡 異쒕젰?쒕떎.  
  - ?? `preflight_outcome=pass_no_position_allowed`(dry-run 臾댄룷吏???덉슜 ?듦낵), `preflight_outcome=fail_real_order_blocked`(?ㅼ＜臾?寃쎈줈 李⑤떒)
- notifier??諛섎뱶???쒖꽦/?ㅼ젙?섏뼱????`NOTIFIER_WEBHOOK_URL` ?먮뒗 `SLACK_WEBHOOK_URL` ?먮뒗 `TELEGRAM_BOT_TOKEN`+`TELEGRAM_CHAT_ID`)
- `KILL_SWITCH_LIQUIDATE`??live preflight ?ㅽ뙣 ?ъ쑀媛 ?꾨땲硫? kill switch ?숈옉 ??flatten ?쒕룄 ?щ?瑜??쒖뼱?쒕떎

?ㅼ＜臾??꾪솚 ?덉감(arming):

1. `LIVE_DRY_RUN=true` ?곹깭濡?濡쒓렇/?뚮┝/蹂듦뎄 ?숈옉??癒쇱? 寃利?2. ?ㅼ＜臾??쒖옉 吏곸쟾??`LIVE_DRY_RUN=false` + `LIVE_REAL_ORDER_ARMED=true`瑜??④퍡 ?ㅼ젙
3. `LIVE_REAL_ORDER_ARMED=true`媛 ?놁쑝硫?live preflight?먯꽌 利됱떆 醫낅즺(fail-fast)


Live 蹂댁닔 preset ?덉떆(?뚯븸 怨꾩젙 + ?뚮┝ ?ы븿):

```bash
# 1) paper?먯꽌 癒쇱? 寃利?(DB 遺꾨━)
MODE=paper DB_PATH=/var/lib/bithumb-bot/data/paper/trades/paper.small.safe.sqlite \
NOTIFIER_ENABLED=true SLACK_WEBHOOK_URL=https://hooks.slack.com/services/xxx/yyy/zzz \
MAX_ORDER_KRW=30000 MAX_DAILY_LOSS_KRW=20000 MAX_DAILY_ORDER_COUNT=6 \
uv run bithumb-bot run

# 2) live dry-run (?ㅼ＜臾?API 誘명샇異?
MODE=live DB_PATH=/var/lib/bithumb-bot/data/live/trades/live.small.safe.sqlite LIVE_DRY_RUN=true LIVE_REAL_ORDER_ARMED=false \
NOTIFIER_ENABLED=true SLACK_WEBHOOK_URL=https://hooks.slack.com/services/xxx/yyy/zzz \
MAX_ORDER_KRW=30000 MAX_DAILY_LOSS_KRW=20000 MAX_DAILY_ORDER_COUNT=6 \
uv run bithumb-bot run

# 3) ?ㅼ＜臾?arming (?댁쁺??紐낆떆 ?뱀씤 ??吏곸쟾?먮쭔)
MODE=live DB_PATH=/var/lib/bithumb-bot/data/live/trades/live.small.safe.sqlite LIVE_DRY_RUN=false LIVE_REAL_ORDER_ARMED=true \
NOTIFIER_ENABLED=true SLACK_WEBHOOK_URL=https://hooks.slack.com/services/xxx/yyy/zzz \
MAX_ORDER_KRW=30000 MAX_DAILY_LOSS_KRW=20000 MAX_DAILY_ORDER_COUNT=6 \
BITHUMB_API_KEY=... BITHUMB_API_SECRET=... \
uv run bithumb-bot run
```

## Health / Recovery Reading

- Runtime artifacts such as health/recovery reports and snapshots belong under env-injected runtime roots, not repo-relative paths like `./data`, `./backups`, or `./tmp`.
- `health` is a live status snapshot, not a green/red verdict. Read `trading_enabled`, `halt_new_orders_blocked`, `unresolved_open_order_count`, and `recovery_required_count` together before deciding to resume.
- Dust vocabulary:
  - `harmless_dust`: broker/local remainder is matched closely enough to be policy-classified as harmless dust. It may still be a real BTC remainder.
  - `unsafe dust` / `mismatch dust`: any dust-like remainder that is not policy-approved to resume, including broker/local mismatch, one-sided dust, or recovery-unclear residue.
  - `effective flat`: the residual is treated as flat by the strategy entry gate. This is a trading interpretation, not a literal zero-balance claim.
  - `resume allowed` / `new orders allowed`: for dust, both become true only when the harmless matched residual is also policy-approved to resume.
- `effective_flat_due_to_harmless_dust=1` can still mean a real BTC remainder exists. Treat it as an operator interpretation of a small remainder, not a literal zero-balance claim.
- `dust_state=harmless_dust` means broker/local dust is close enough to be harmless under policy. It is only resume-safe when the matching policy also allows resume, and fresh BUYs are allowed only when `dust_new_orders_allowed=1` as well.
- `dust_state=dangerous_dust` means the remainder is not safely resumable yet. Keep new orders blocked until the broker, DB, and recovery evidence all line up.
- `accounts_flat_start_allowed=True` is only a `/v1/accounts` diagnostic. It does not override `recovery-report` blockers.
- `order_rules_autosync=FALLBACK` means `/v1/orders/chance` rule data was not available and the bot is using local fallback constraints. In live mode, clear that warning before real-order arming.
- `/v2/orders` pre-validation still applies after the rule snapshot is loaded: market buys are `side=bid, order_type=price, price=<KRW>`, and market sells are `side=ask, order_type=market, volume=<qty>`.

## Dust Residual Operational Reading

- `dust residual` means the remaining BTC is small enough that one or both exchange sell gates may fail: minimum quantity and minimum notional. "Dust" is about sellability, not about whether order recovery is complete.
- `matched dust` means broker/app balance and local DB balance match closely enough, and the remaining position is classified as `harmless_dust`. Legacy reconcile metadata may still use `matched_harmless_dust_*` for the same class. That label can still mean a real BTC remainder exists; it is an operator reading that the remainder is harmless enough to resume only when policy also allows it.
- `dangerous dust` means the remainder is classified as `dangerous_dust`: broker/local quantities do not match safely, or the quantity is small but still operationally risky.
- `unresolved order` means order lifecycle consistency is still unclear. This is not the same as dust and must be treated as a higher-risk condition.
- Before restart, check in this order:
  1. `uv run bithumb-bot health`
  2. `uv run bithumb-bot recovery-report`
  3. `uv run bithumb-bot ops-report --limit 20`
- Read the outputs in this order:
  1. `recovery-report [P1]` and `[P2]` decide whether restart is allowed.
  2. `recovery-report [P3.0]` explains whether any remaining position is dust-only or restart-blocking dust.
  3. `ops-report` is the operator cross-check for dust numbers and `/v1/accounts` diagnostics.
- `resume_allowed=0` and `can_resume=false` always mean do not restart yet. If the blocker list includes `MATCHED_DUST_POLICY_REVIEW_REQUIRED` or `DANGEROUS_DUST_REVIEW_REQUIRED`, treat that as a dust policy gate, not as proof of unresolved orders.
- `accounts_flat_start_allowed=True` is only a `/v1/accounts` preflight diagnostic. It never overrides `recovery-report` restart blockers.
- If `dust_state=dangerous_dust`, treat the bot as restart-blocked for new orders even when `/v1/accounts` diagnostics say `accounts_flat_start_allowed=True`.
- If `dust_state=harmless_dust`, the remainder may be operationally flat only when all of these are true:
  1. `recovery-report` shows `unresolved_count=0`
  2. `recovery-report` shows `recovery_required_count=0`
  3. `recovery-report [P3.0]` shows `allow_resume=1` and `resume_allowed_by_policy=1`
- If you want a document-only answer to "can I place a new BUY?", use this rule:
  - allowed only when `dust_state=harmless_dust`, `dust_new_orders_allowed=1`, `dust_resume_allowed_by_policy=1`, `dust_treat_as_flat=1`, and `effective_flat_due_to_harmless_dust=1`
  - blocked in every other dust case, including harmless dust under review
- If `unresolved_count > 0` or `recovery_required_count > 0`, do not downgrade the situation to dust-only until recovery evidence is clear.
- For manual review, compare three views before any resume decision:
  1. app view: `health` / `recovery-report` / `ops-report` dust fields
  2. DB view: local position and recent sell evidence represented by `dust_local_qty`, `recent_dust_unsellable_event`, unresolved counts, and recovery-required counts
  3. broker view: `/v1/accounts` diagnostics and broker quantity represented by `dust_broker_qty`
- `dust_broker_qty` and `dust_local_qty` should be read together with `dust_broker_local_match`. A small remainder is only resume-safe when the broker/local remainder matches closely enough and the policy marks it resume-safe.
- `dust_min_qty` and `dust_min_notional_krw` are different gates. A sell can be blocked because quantity is below minimum, because notional is below minimum, or because both are below minimum. Do not assume one implies the other, and do not assume a remainder above one minimum is tradable before checking the other.
- `effective_flat_due_to_harmless_dust=1` is a reporting convenience, not proof of a literal zero balance. If the bot is in this state, keep reading the broker/DB quantities before deciding whether the position is actually flat.
- Strategy `position.in_position` follows the entry gate, not the raw dust label. When harmless dust is policy-approved and marked effective flat, strategy may report `in_position=False` even though a small BTC remainder still exists.

## Manual App Sell Caution

- Operating direction: do not rely on manual app sells as the normal dust-handling path. The preferred path is `reconcile` plus report review, then resume only if policy allows it.
- If the bot is stopped and you manually sell in the exchange app, run `health`, `recovery-report`, and `ops-report` again before restarting.
- Manual app sells can leave dust smaller than exchange minimums. In that case, another sell attempt may fail or create misleading operator signals.
- Before any manual sell retry, confirm all of the following:
  1. `unresolved_count=0` and `recovery_required_count=0`
  2. broker/local dust numbers are understood from `dust_broker_qty`, `dust_local_qty`, and `dust_broker_local_match`
  3. both exchange limits are checked: `dust_min_qty` and `dust_min_notional_krw`
  4. the intended sell size is actually above both minimums after quantity-step and decimal normalization
- Do not assume "almost zero balance" means restart is safe. Confirm `dust_state`, `dust_action`, `dust_resume_allowed_by_policy`, and unresolved order counts first.
- If `dust_state=dangerous_dust`, resume is not allowed. Reconcile, compare app/DB/broker state, and keep new orders blocked until the residual is explained.
- If `dust_state=harmless_dust` but `dust_resume_allowed_by_policy=0`, treat it as a review-required matched dust case: exposure can be treated as flat for interpretation, but restart and new orders still stay blocked.
- Do not use `resume --force` as a shortcut around dust review. First confirm this is dust only and not an unresolved order or mismatched broker/local state.
- Prefer `reconcile` plus report review over `resume --force` whenever broker balance changed outside the bot.

