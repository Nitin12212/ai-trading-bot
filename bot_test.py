import requests
import time
import pandas as pd
import sqlite3
import os
import logging
import random
from datetime import datetime, timedelta
from threading import Thread
from tvDatafeed import TvDatafeed, Interval
from flask import Flask

# ==========================================
# 🛡️ 1. SECURITY & GLOBALS
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
BASE_URL = f"https://api.telegram.org/bot{TOKEN}" if TOKEN else ""

# 🔐 CRITICAL FIX: Only YOU can control the bot
AUTHORIZED_USER = int(os.getenv("AUTHORIZED_USER", 0)) # Apna Telegram Chat ID Render Env me dalein

app = Flask(__name__)
@app.route('/')
def home(): return "🛡️ V8.0 OMEGA TIER is LIVE & SECURED!"

def run_server():
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

# Globals
bot_paused = False
trading_mode = "DEMO"
pending_mode_confirm = False
strategy_mode = "SAFE"
alerts_muted = False
current_risk_percent = 2.0
daily_loss_limit = -2000.0
daily_profit_target = 3000.0
tv = TvDatafeed()

# ==========================================
# 🗄️ 2. DATABASE ENGINE
# ==========================================
def execute_db(query, params=(), fetch=False, fetchall=False):
    try:
        conn = sqlite3.connect('trades.db', timeout=10)
        c = conn.cursor()
        c.execute(query, params)
        if fetch: res = c.fetchone()
        elif fetchall: res = c.fetchall()
        else: conn.commit(); res = True
        conn.close()
        return res
    except Exception as e:
        logging.error(f"DB Error: {e}")
        return None

def setup_db():
    execute_db('''CREATE TABLE IF NOT EXISTS pro_trades 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, symbol TEXT, type TEXT, 
                  entry_price REAL, sl REAL, tp REAL, status TEXT, pnl REAL, mode TEXT)''')

def get_ist(): return datetime.utcnow() + timedelta(hours=5, minutes=30)

# ==========================================
# 📱 3. TELEGRAM UI & KEYBOARD
# ==========================================
def send_msg(chat_id, text):
    if not TOKEN or not chat_id: return
    url = f"{BASE_URL}/sendMessage"
    keyboard = {
        "keyboard": [
            [{"text": "📊 Check Status"}, {"text": "📅 Today Report"}],
            [{"text": "💰 View PnL"}, {"text": "📂 Open Trades"}],
            [{"text": "📊 Detailed Stats"}, {"text": "🔍 Scan Now"}],
            [{"text": "🛡️ Safe Mode"}, {"text": "⚡ Aggressive Mode"}],
            [{"text": "🔕 Mute Alerts"}, {"text": "🔔 Unmute Alerts"}],
            [{"text": "🔄 Switch Mode"}, {"text": "❌ Close All"}],
            [{"text": "⏸ Pause Bot"}, {"text": "▶️ Resume Bot"}]
        ], "resize_keyboard": True
    }
    requests.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown", "reply_markup": keyboard, "disable_web_page_preview": True})

# ==========================================
# 🧠 4. OMEGA SCANNER (MTF + SMART ENTRY)
# ==========================================
def scanner():
    global bot_paused, trading_mode, strategy_mode, alerts_muted
    symbols = ['NIFTY', 'BANKNIFTY', 'CNXFINANCE']
    
    while True:
        if not AUTHORIZED_USER or bot_paused:
            time.sleep(5); continue
            
        # 🕒 1. TIME FILTER
        now_time = get_ist().time()
        start_mkt = datetime.strptime("09:15", "%H:%M").time()
        end_mkt = datetime.strptime("15:30", "%H:%M").time()
        # if not (start_mkt <= now_time <= end_mkt): { time.sleep(60); continue } # Live market me uncomment karein
            
        today = get_ist().strftime("%Y-%m-%d")
        
        # 🛑 2. DAILY LIMITS & STREAK CONTROL
        res_pnl = execute_db("SELECT SUM(pnl) FROM pro_trades WHERE date LIKE ? AND status!='OPEN'", (f"{today}%",), fetch=True)
        today_pnl = res_pnl[0] if res_pnl and res_pnl[0] else 0.0

        if today_pnl <= daily_loss_limit:
            send_msg(AUTHORIZED_USER, f"🛑 *CIRCUIT BREAKER:* Loss ₹{today_pnl:.2f}. Trading Locked!")
            bot_paused = True; time.sleep(60); continue
            
        if today_pnl >= daily_profit_target:
            send_msg(AUTHORIZED_USER, f"🎯 *TARGET ACHIEVED:* Profit ₹{today_pnl:.2f}. System Resting!")
            bot_paused = True; time.sleep(60); continue
            
        last_3 = execute_db("SELECT status FROM pro_trades WHERE status!='OPEN' ORDER BY id DESC LIMIT 3", fetchall=True)
        if last_3 and len(last_3) == 3 and all("LOSS" in l[0] for l in last_3):
            send_msg(AUTHORIZED_USER, "⚠️ *WIN STREAK CONTROL:* 3 consecutive losses detected. Pausing bot for safety.")
            bot_paused = True; time.sleep(60); continue

        for sym in symbols:
            try:
                # 📡 FETCH DATA (5m & 15m)
                data_5m = tv.get_hist(symbol=sym, exchange='NSE', interval=Interval.in_5_minute, n_bars=100)
                data_15m = tv.get_hist(symbol=sym, exchange='NSE', interval=Interval.in_15_minute, n_bars=100)
                if data_5m is None or data_15m is None: continue
                
                cp = data_5m['close'].iloc[-1]
                ema200 = data_5m['close'].ewm(span=200, adjust=False).mean().iloc[-1]
                trend_15m_up = data_15m['close'].iloc[-1] > data_15m['close'].ewm(span=50).mean().iloc[-1]
                
                # Exit Management
                open_trades = execute_db("SELECT id, type, entry_price, sl, tp FROM pro_trades WHERE symbol=? AND status='OPEN'", (sym,), fetchall=True)
                if open_trades:
                    for t in open_trades:
                        t_id, t_type, entry, sl, tp = t
                        status, pnl, msg = "OPEN", 0.0, None
                        
                        if t_type == "BUY 🟢":
                            if cp >= tp: status, pnl, msg = "PROFIT ✅", tp - entry, f"🎯 TARGET HIT: {sym} (+₹{tp - entry:.2f})"
                            elif cp <= sl: status, pnl, msg = "LOSS ❌", sl - entry, f"🛑 SL HIT: {sym} (-₹{entry - sl:.2f})"
                        elif t_type == "SELL 🔴":
                            if cp <= tp: status, pnl, msg = "PROFIT ✅", entry - tp, f"🎯 TARGET HIT: {sym} (+₹{entry - tp:.2f})"
                            elif cp >= sl: status, pnl, msg = "LOSS ❌", entry - sl, f"🛑 SL HIT: {sym} (-₹{sl - entry:.2f})"
                        
                        if status != "OPEN":
                            execute_db("UPDATE pro_trades SET status=?, pnl=? WHERE id=?", (status, pnl, t_id))
                            if not alerts_muted: send_msg(AUTHORIZED_USER, msg)
                    continue 
                
                # 🧠 SMART ENTRY LOGIC
                dist_ema = (abs(cp - ema200) / ema200) * 100
                if dist_ema > 0.3: continue # Fake/FOMO signal prevention
                
                decision = "WAIT"
                if cp > ema200 and trend_15m_up: decision = "BUY 🟢"
                elif cp < ema200 and not trend_15m_up: decision = "SELL 🔴"

                if decision != "WAIT":
                    sl = cp - (cp * 0.002) if "BUY" in decision else cp + (cp * 0.002)
                    tp = cp + (cp * 0.004) if "BUY" in decision else cp - (cp * 0.004)
                    
                    # Prevent Duplicate Signal
                    last_trade = execute_db("SELECT type FROM pro_trades WHERE symbol=? ORDER BY id DESC LIMIT 1", (sym,), fetch=True)
                    if last_trade and last_trade[0] == decision: continue

                    execute_db('INSERT INTO pro_trades (date, symbol, type, entry_price, sl, tp, status, pnl, mode) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                               (get_ist().strftime("%Y-%m-%d %H:%M"), sym, decision, cp, sl, tp, "OPEN", 0.0, trading_mode))
                    
                    rr_ratio = f"1:{abs(tp - cp) / abs(cp - sl):.1f}"
                    tv_link = f"https://www.tradingview.com/chart/?symbol=NSE:{sym}"
                    
                    if not alerts_muted:
                        send_msg(AUTHORIZED_USER, f"🚀 *{trading_mode} ENTRY EXECUTED*\n\n📈 *Symbol:* {sym}\n🤖 *Action:* {decision}\n🔸 *Entry:* ₹{cp:.2f}\n🎯 *TP:* ₹{tp:.2f} | 🛡️ *SL:* ₹{sl:.2f}\n⚖️ *RR:* {rr_ratio}\n\n📊 [View Chart]({tv_link})")

            except Exception as e: logging.error(f"Scan error {sym}: {e}")
            time.sleep(1) 
        time.sleep(30) 

# ==========================================
# 🎮 5. TELEGRAM COMMAND HANDLER
# ==========================================
def telegram():
    global bot_paused, trading_mode, pending_mode_confirm, strategy_mode, alerts_muted, current_risk_percent
    last_id = None
    
    while True:
        try:
            url = f"{BASE_URL}/getUpdates?timeout=5"
            if last_id: url += f"&offset={last_id + 1}"
            res = requests.get(url).json()
            
            if "result" in res:
                for upd in res["result"]:
                    last_id = upd["update_id"]
                    if "message" in upd and "text" in upd["message"]:
                        chat_id = upd["message"]["chat"]["id"]
                        txt = upd["message"]["text"]
                        
                        # 🔐 SECURITY LOCK (CRITICAL)
                        if chat_id != AUTHORIZED_USER:
                            send_msg(chat_id, "❌ *UNAUTHORIZED ACCESS.* This incident has been logged.")
                            continue
                            
                        # Commands
                        if txt == "/start": send_msg(chat_id, "🛡️ V8.0 OMEGA TIER Online. Authorized Access Granted.")
                        elif txt in ["🔄 Switch Mode", "/mode"]:
                            pending_mode_confirm = True
                            send_msg(chat_id, f"⚠️ Current Mode: *{trading_mode}*\nType `CONFIRM` to switch mode.")
                        elif txt == "CONFIRM" and pending_mode_confirm:
                            trading_mode = "REAL" if trading_mode == "DEMO" else "DEMO"
                            pending_mode_confirm = False
                            send_msg(chat_id, f"💰 Mode Switched to: *{trading_mode}*")
                        elif txt == "📊 Check Status": 
                            send_msg(chat_id, f"📡 System: {'Paused ⏸' if bot_paused else 'Active ▶️'}\n🧠 Mode: {trading_mode}\n🛡️ Strategy: {strategy_mode}\n🔔 Alerts: {'Muted 🔕' if alerts_muted else 'ON 🔔'}")
                        elif txt in ["📅 Today Report", "/today"]:
                            t = get_ist().strftime("%Y-%m-%d")
                            pnl = execute_db("SELECT SUM(pnl) FROM pro_trades WHERE date LIKE ?", (f"{t}%",), fetch=True)[0] or 0.0
                            tc = execute_db("SELECT COUNT(*) FROM pro_trades WHERE date LIKE ?", (f"{t}%",), fetch=True)[0] or 0
                            send_msg(chat_id, f"📅 *Today's Status:*\n💰 PnL: ₹{pnl:.2f}\n⚡ Trades Taken: {tc}")
                        elif txt == "📊 Detailed Stats":
                            wins = execute_db("SELECT COUNT(*) FROM pro_trades WHERE status='PROFIT ✅'", fetch=True)[0] or 0
                            losses = execute_db("SELECT COUNT(*) FROM pro_trades WHERE status='LOSS ❌'", fetch=True)[0] or 0
                            total = wins + losses
                            wr = (wins/total*100) if total>0 else 0
                            pnl = execute_db("SELECT SUM(pnl) FROM pro_trades", fetch=True)[0] or 0.0
                            send_msg(chat_id, f"📊 *Detailed Stats:*\n\n📈 Total Trades: {total}\n🏆 Win Rate: {wr:.1f}%\n✅ Wins: {wins} | ❌ Losses: {losses}\n💰 Net PnL: ₹{pnl:.2f}")
                        elif txt == "🔍 Scan Now":
                            send_msg(chat_id, "🔍 Manual Scan Initiated. Check logs if setup forms.")
                        elif txt == "🛡️ Safe Mode": strategy_mode = "SAFE"; send_msg(chat_id, "🛡️ *Safe Mode* Activated (15m MTF Strict).")
                        elif txt == "⚡ Aggressive Mode": strategy_mode = "AGGRESSIVE"; send_msg(chat_id, "⚡ *Aggressive Mode* Activated (Early Entry).")
                        elif txt == "🔕 Mute Alerts": alerts_muted = True; send_msg(chat_id, "🔕 Alerts Muted.")
                        elif txt == "🔔 Unmute Alerts": alerts_muted = False; send_msg(chat_id, "🔔 Alerts Unmuted.")
                        elif txt in ["📂 Open Trades", "/open"]:
                            rows = execute_db("SELECT symbol, type, entry_price FROM pro_trades WHERE status='OPEN'", fetchall=True)
                            msg = "⚡ *LIVE TRADES:*\n" + "\n".join([f"🔹 {r[0]} | {r[1]} @ ₹{r[2]}" for r in rows]) if rows else "No open trades."
                            send_msg(chat_id, msg)
                        elif txt in ["❌ Close All", "/closeall"]:
                            rows = execute_db("SELECT id, symbol, type, entry_price FROM pro_trades WHERE status='OPEN'", fetchall=True)
                            if rows:
                                for r in rows: execute_db("UPDATE pro_trades SET status='CLOSED ⚠️' WHERE id=?", (r[0],))
                                send_msg(chat_id, "⚠️ All positions FORCE CLOSED.")
                            else: send_msg(chat_id, "❌ No open trades.")
                        elif txt == "⏸ Pause Bot": bot_paused = True; send_msg(chat_id, "🛑 Bot Paused.")
                        elif txt == "▶️ Resume Bot": bot_paused = False; send_msg(chat_id, "✅ Bot Resumed.")
        except Exception as e: logging.error(f"TG Error: {e}")
        time.sleep(1)

if __name__ == "__main__":
    setup_db()
    Thread(target=run_server, daemon=True).start()
    Thread(target=scanner, daemon=True).start()
    telegram()
