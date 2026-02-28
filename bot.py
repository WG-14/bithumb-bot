diff --git a/bot.py b/bot.py
index cef3e4864060c2db3b9782ccf6a09d13d1f9282f..b1f4f912635ac8b45afdaa7dc869d26dd6f26547 100644
--- a/bot.py
+++ b/bot.py
@@ -7,50 +7,54 @@ from datetime import datetime, timezone, timedelta
 
 import httpx
 from dotenv import load_dotenv
 
 # .env를 bot.py 위치 기준으로 로드
 load_dotenv(Path(__file__).with_name(".env"))
 
 BASE_URL = "https://api.bithumb.com"
 
 PAIR = os.getenv("PAIR", "BTC_KRW")
 INTERVAL = os.getenv("INTERVAL", "1m")
 DB_PATH = os.getenv("DB_PATH", "data/bithumb.sqlite")
 
 MODE = os.getenv("MODE", "paper").lower()                     # paper / live(미사용)
 ENTRY_MODE = os.getenv("ENTRY_MODE", "cross").lower()         # cross / regime
 
 START_CASH_KRW = float(os.getenv("START_CASH_KRW", "1000000"))
 FEE_RATE = float(os.getenv("FEE_RATE", "0.0004"))
 BUY_FRACTION = float(os.getenv("BUY_FRACTION", "0.99"))
 
 SMA_SHORT = int(os.getenv("SMA_SHORT", "7"))
 SMA_LONG = int(os.getenv("SMA_LONG", "30"))
 COOLDOWN_BARS = int(os.getenv("COOLDOWN_BARS", "0"))
 MIN_GAP = float(os.getenv("MIN_GAP", "0.0"))  # 예: 0.0003 = 0.03%
 
+
+def format_pct(v: float) -> str:
+    return f"{v * 100:.4f}%"
+
 # ---------- utilities ----------
 def fetch_json(path: str):
     url = f"{BASE_URL}{path}"
     with httpx.Client(timeout=10) as client:
         r = client.get(url)
         r.raise_for_status()
         return r.json()
 
 
 def ensure_db() -> sqlite3.Connection:
     p = Path(DB_PATH)
     p.parent.mkdir(parents=True, exist_ok=True)
     conn = sqlite3.connect(p)
 
     conn.execute(
         """
         CREATE TABLE IF NOT EXISTS candles (
             pair TEXT NOT NULL,
             interval TEXT NOT NULL,
             ts INTEGER NOT NULL,          -- epoch ms
             open REAL NOT NULL,
             close REAL NOT NULL,
             high REAL NOT NULL,
             low REAL NOT NULL,
             volume REAL NOT NULL,
@@ -340,98 +344,156 @@ def cmd_run(short_n: int, long_n: int, entry_mode: str):
             # 캔들 마감 직후를 노리고 약간 늦게(2초)
             now = time.time()
             sleep_s = sec - (now % sec) + 2
             time.sleep(sleep_s)
 
             cnt, _ = sync_candles(long_n)
             r = compute_signal(short_n, long_n)
             if r is None:
                 print(f"[RUN] 데이터 부족 (db_count={cnt})")
                 continue
 
             # 같은 캔들은 재처리하지 않기
             if last_processed_ts == r["ts"]:
                 continue
             last_processed_ts = r["ts"]
 
             # 현재 포트폴리오 확인
             conn = ensure_db()
             init_portfolio(conn)
             cash, qty = get_portfolio(conn)
             conn.close()
 
             # -----------------------------
             # (1) 필터 1: 쿨다운
             # -----------------------------
+            reason = ""
             if cooldown_left > 0:
                 cooldown_left -= 1
                 action = "HOLD"
+                reason = f"cooldown({cooldown_left} bars left)"
             else:
                 # -----------------------------
                 # (2) 필터 2: SMA 간격(min-gap)
                 # -----------------------------
                 gap = abs(r["curr_s"] - r["curr_l"]) / r["curr_l"]  # 비율
                 if gap < MIN_GAP:
                     action = "HOLD"
+                    reason = f"gap<{format_pct(MIN_GAP)} (now={format_pct(gap)})"
                 else:
                     # -----------------------------
                     # (3) 기존 entry_mode 로직 (여기가 “action 결정”)
                     # -----------------------------
                     if entry_mode == "cross":
                         action = r["signal"]  # BUY/SELL/HOLD (교차 순간)
+                        if action == "HOLD":
+                            reason = "no crossover on this closed candle"
+                        else:
+                            reason = f"crossover={action}"
                     elif entry_mode == "regime":
                         # short>long이면 롱 상태 유지, short<long이면 현금 상태 유지
                         if r["above"] and qty <= 0.0:
                             action = "BUY"
+                            reason = "regime=above and no position"
                         elif (not r["above"]) and qty > 0.0:
                             action = "SELL"
+                            reason = "regime=below and have position"
                         else:
                             action = "HOLD"
+                            if r["above"] and qty > 0.0:
+                                reason = "regime=above and already in position"
+                            elif (not r["above"]) and qty <= 0.0:
+                                reason = "regime=below and already in cash"
+                            else:
+                                reason = "regime unchanged"
                     else:
                         raise ValueError("entry_mode must be 'cross' or 'regime'")
 
             print(
                 f"[RUN] {kst_str(r['ts'])} close={r['last_close']:,.0f} "
                 f"SMA{short_n}={r['curr_s']:.2f} SMA{long_n}={r['curr_l']:.2f} "
-                f"(cross={r['signal']}) cooldown_left={cooldown_left} => {action}"
+                f"(cross={r['signal']}) cooldown_left={cooldown_left} => {action} ({reason})"
             )
 
             if MODE == "paper" and action in ("BUY", "SELL"):
                 trade = paper_execute(action, r["ts"], r["last_close"])
                 if trade:
                     # ✅ 거래가 실제로 발생했을 때만 쿨다운 시작
                     cooldown_left = COOLDOWN_BARS
                     side, t_qty, fee, cash_after, asset_after = trade
                     print(
                         f"  [PAPER] {side} qty={t_qty:.8f} fee={fee:,.0f} "
                         f"cash={cash_after:,.0f} asset={asset_after:.8f}"
                     )
 
     except KeyboardInterrupt:
         print("\n[RUN] stopped (Ctrl+C)")
 
+
+def cmd_advise(short_n: int, long_n: int, entry_mode: str):
+    print("[ADVISE] HOLD가 길게 나올 때 점검 순서")
+    print("1) 파라미터 확인")
+    print(f"   ENTRY_MODE={entry_mode}, short={short_n}, long={long_n}, MIN_GAP={MIN_GAP}, COOLDOWN_BARS={COOLDOWN_BARS}")
+
+    conn = ensure_db()
+    init_portfolio(conn)
+    cash, qty = get_portfolio(conn)
+    conn.close()
+    print(f"2) 포트폴리오 상태: cash={cash:,.0f} KRW, qty={qty:.8f}")
+
+    r = compute_signal(short_n, long_n)
+    if r is None:
+        print("3) 데이터 부족: run을 조금 더 실행해 캔들을 누적하세요.")
+        return
+
+    gap = abs(r["curr_s"] - r["curr_l"]) / r["curr_l"]
+    print(f"3) 최근 지표: cross={r['signal']} above={r['above']} gap={format_pct(gap)}")
+
+    print("4) 권장 액션")
+    if entry_mode == "cross":
+        print("   - cross 모드는 교차 '순간'만 진입/청산하므로 HOLD가 자주 나옵니다.")
+        print("   - 체결 빈도를 늘리려면 --entry regime 또는 short/long 간격 축소(예: 2/5, 3/10)를 시도하세요.")
+    else:
+        print("   - regime 모드는 상태 유지형이라 cross보다 체결이 늘어납니다.")
+
+    if MIN_GAP > 0:
+        print("   - MIN_GAP이 크면 HOLD가 늘어납니다. 페이퍼 테스트 단계에서는 0~0.0002 권장.")
+    if COOLDOWN_BARS > 0:
+        print("   - COOLDOWN_BARS가 크면 거래 후 N개 봉 동안 HOLD 고정됩니다.")
+
+    print("5) 바로 실행 예시")
+    print(f"   python bot.py run --short {short_n} --long {long_n} --entry {entry_mode}")
+    print("   python bot.py run --short 2 --long 5 --entry regime")
+
 def main():
     p = argparse.ArgumentParser()
     sub = p.add_subparsers(dest="cmd", required=True)
 
     sub.add_parser("status")
 
     t = sub.add_parser("trades")
     t.add_argument("--limit", type=int, default=20)
 
     r = sub.add_parser("run")
     r.add_argument("--short", type=int, default=SMA_SHORT)
     r.add_argument("--long", type=int, default=SMA_LONG)
     r.add_argument("--entry", choices=["cross", "regime"], default=ENTRY_MODE)
 
+    a = sub.add_parser("advise")
+    a.add_argument("--short", type=int, default=SMA_SHORT)
+    a.add_argument("--long", type=int, default=SMA_LONG)
+    a.add_argument("--entry", choices=["cross", "regime"], default=ENTRY_MODE)
+
     args = p.parse_args()
 
     if args.cmd == "status":
         cmd_status()
     elif args.cmd == "trades":
         cmd_trades(args.limit)
     elif args.cmd == "run":
         cmd_run(args.short, args.long, args.entry)
+    elif args.cmd == "advise":
+        cmd_advise(args.short, args.long, args.entry)
 
 
 if __name__ == "__main__":
-    main()
\ No newline at end of file
+    main()
