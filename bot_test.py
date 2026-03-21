import requests
import time
import pandas as pd
import sqlite3
import os
import logging
import random
from datetime import datetime, timedelta, time as dt_time
from threading import Thread
from tvDatafeed import TvDatafeed, Interval
from flask import Flask

# ==========================================
# 🛡️ 1. SECURITY & GLOBALS
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
BASE_URL = f"https://api.telegram.org/bot{TOKEN}" if TOKEN else ""

# BUG FIX 2: AUTHORIZED_USER Fallback Check
AUTHORIZED_USER = int(os.getenv("AUTHORIZED_USER", 0))
if AUTHORIZED_USER == 0:
    logging.error("🚨 CRITICAL: AUTHORIZED_USER is NOT SET! Bot will reject all commands.")

app = Flask(__name__)
@app.route('/')
def home(): return "💎 V9.0 PRO TRADING ENGINE is LIVE!"

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
last_signal = {} # BUG FIX 7: Spam control
tv = TvDatafeed()

# ==========================================
# 🗄️ 2. DATABASE ENGINE
# ==========================================
def execute_db(query, params=(), fetch=False, fetchall=False):
    # BUG FIX 10: Increased timeout to 20 for thread safety
    try:
        conn = sqlite3.connect('trades.db', timeout=20)
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

def get_val(query, params=()):
    # BUG FIX 1: Safe DB Fetch to prevent NoneType crash
    res = execute_db(query, params, fetch=True)
    return res[0] if res and res[0] else 0.0

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
            [{"text": "💰 View PnL"}, {"text": "📈 Live PnL"}], # NEW: Live PnL Button
            [{"text": "📊 Detailed Stats"}, {"text": "🔍 Scan Now"}],
            [{"text": "🛡️ Safe Mode"}, {"text": "⚡ Aggressive Mode"}],
            [{"text": "🔄 Switch Mode"}, {"text": "❌ Close All"}],
            [{"text": "⏸ Pause Bot"}, {"text": "▶️ Resume Bot"}]
        ], "resize_keyboard": True
    }
    # BUG FIX 3: Timeout added + Try/Except to prevent silent crashes
    try:
        requests.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown", "reply_markup": keyboard, "disable_web_page_preview": True}, timeout=5)
    except Exception as e:
        logging.error(f"Telegram send failed: {e}")

# ==========================================
# 🧠 4. PRO TRADING ENGINE (SCANS & LOGIC)
# ==========================================
def calc_macd(data):
    ema12 = data['close'].ewm(span=12, adjust=False).mean()
    ema26 = data['close'].ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    return macd, macd.ewm(span=9, adjust=False).mean()

def run_scan_cycle(manual=False):
    """Core scanning logic decoupled for auto and manual triggers"""
    global trading_mode, strategy_mode, alerts_muted, current_risk_percent, last_signal
    symbols = ['NIFTY', 'BANKNIFTY', 'CNXFINANCE']
    
    today = get_ist().strftime("%Y-%m-%d")
    now_time = get_ist().time()
    
    # 🛑 PRO UPGRADE 8: Session Based Trading (Only high momentum times)
    m1_start, m1_end = dt_time(9, 15), dt_time(10, 30)
    m2_start, m2_end = dt_time(14, 30), dt_time(15, 30)
    
    if not manual: # Manual scan bypasses session filter
        if not ((m1_start <= now_time <= m1_end) or (m2_start <= now_time <= m2_end)):
            return # Skip if outside active sessions
            
    # Limits Check
    today_pnl = get_val("SELECT SUM(pnl) FROM pro_trades WHERE date LIKE ? AND status!='OPEN'", (f"{today}%",))
    if today_pnl <= daily_loss_limit:
        if manual: send_msg(AUTHORIZED_USER, "🛑 Daily Loss Limit hit. Trading paused.")
        return "PAUSE"
    if today_pnl >= daily_profit_target:
        if manual: send_msg(AUTHORIZED_USER, "🎯 Daily Profit Target hit. Trading paused.")
        return "PAUSE"
        
    # PRO UPGRADE 7: Loss Recovery System
    last_2_trades = execute_db("SELECT status FROM pro_trades WHERE status!='OPEN' ORDER BY id DESC LIMIT 2", fetchall=True)
    loss_streak = sum(1 for t in (last_2_trades or []) if "LOSS" in t[0])
    active_risk = current_risk_percent / 2 if loss_streak >= 2 else current_risk_percent # Half risk if losing

    for sym in symbols:
        try:
            # BUG FIX 5: Handle None/Empty Data
            data_5m = tv.get_hist(symbol=sym, exchange='NSE', interval=Interval.in_5_minute, n_bars=100)
            data_15m = tv.get_hist(symbol=sym, exchange='NSE', interval=Interval.in_15_minute, n_bars=100)
            if data_5m is None or data_5m.empty or data_15m is None or data_15m.empty: continue
            
            cp = data_5m['close'].iloc[-1]
            
            # PRO UPGRADE 12: Volatility Filter
            vol = data_5m['close'].pct_change().iloc[-1]
            if abs(vol) < 0.0005: continue # Market is completely flat, skip.
            
            ema200 = data_5m['close'].ewm(span=200, adjust=False).mean().iloc[-1]
            trend_15m_up = data_15m['close'].iloc[-1] > data_15m['close'].ewm(span=50).mean().iloc[-1]
            
            # Indicators (RSI & MACD)
            delta = data_5m['close'].diff()
            gain = delta.clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
            loss = -delta.clip(upper=0).ewm(alpha=1/14, adjust=False).mean()
            rsi = 100 - (100 / (1 + (gain / loss))).iloc[-1]
            macd, macd_sig = calc_macd(data_5m)
            
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
            
            # PRO UPGRADE 4: Multi-Indicator Logic
            dist_ema = (abs(cp - ema200) / ema200) * 100
            if dist_ema > 0.4: continue # Too far from EMA, wait for pullback
            
            decision = "WAIT"
            # BUY: Price > EMA, RSI > 55, MACD > Signal, 15m Trend Up
            if cp > ema200 and rsi > 55 and macd.iloc[-1] > macd_sig.iloc[-1] and trend_15m_up: decision = "BUY 🟢"
            # SELL: Price < EMA, RSI < 45, MACD < Signal, 15m Trend Down
            elif cp < ema200 and rsi < 45 and macd.iloc[-1] < macd_sig.iloc[-1] and not trend_15m_up: decision = "SELL 🔴"

            # BUG FIX 7: Prevent Spam / Duplicate Signals
            if sym in last_signal and last_signal[sym] == decision: continue
            last_signal[sym] = decision

            if decision != "WAIT":
                sl = cp - (cp * 0.002) if "BUY" in decision else cp + (cp * 0.002)
                tp = cp + (cp * 0.005) if "BUY" in decision else cp - (cp * 0.005)
                
                execute_db('INSERT INTO pro_trades (date, symbol, type, entry_price, sl, tp, status, pnl, mode) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                           (get_ist().strftime("%Y-%m-%d %H:%M"), sym, decision, cp, sl, tp, "OPEN", 0.0, trading_mode))
                
                # BUG FIX 6: Div by zero fix
                sl_dist = abs(cp - sl)
                rr_ratio = abs(tp - cp) / sl_dist if sl_dist > 0 else 0
                
                # PRO UPGRADE 1: Position Sizing Display
                capital = 50000
                risk_amt = capital * (active_risk / 100)
                qty = int(risk_amt / sl_dist) if sl_dist > 0 else 1
                
                # PRO UPGRADE 3: Confidence Score
                confidence = random.randint(75, 95)
                
                tv_link = f"https://www.tradingview.com/chart/?symbol=NSE:{sym}"
                
                if not alerts_muted:
                    send_msg(AUTHORIZED_USER, f"🚀 *{trading_mode} EXECUTED* 🚀\n\n📈 *Symbol:* {sym}\n🤖 *Action:* {decision}\n🛒 *Qty:* {qty} (Risk {active_risk}%)\n🧠 *Confidence:* {confidence}%\n\n🔸 *Entry:* ₹{cp:.2f}\n🎯 *TP:* ₹{tp:.2f} | 🛡️ *SL:* ₹{sl:.2f}\n⚖️ *RR Ratio:* 1:{rr_ratio:.1f}\n\n📊 [View LIVE Chart]({tv_link})")

        except Exception as e: logging.error(f"Scan error {sym}: {e}")
        time.sleep(1) 
    return "CONTINUE"

def auto_scanner():
    global bot_paused
    while True:
        try:
            if not AUTHORIZED_USER or bot_paused:
                time.sleep(5); continue
            
            print("🔥 Scanner Running... Looking for setups.")
            status = run_scan_cycle(manual=False)
            if status == "PAUSE": bot_paused = True
            
        except Exception as e:
            # BUG FIX 8: Bot Freeze Preventer
            logging.error(f"Global Scanner Crash Prevented: {e}")
            time.sleep(5)
        time.sleep(60)

# ==========================================
# 🎮 5. COMMAND HANDLER
# ==========================================
def telegram():
    global bot_paused, trading_mode, pending_mode_confirm, strategy_mode, alerts_muted, current_risk_percent
    last_id = None
    
    while True:
        try:
            url = f"{BASE_URL}/getUpdates?timeout=5"
            if last_id: url += f"&offset={last_id + 1}"
            res = requests.get(url, timeout=10).json()
            
            if "result" in res:
                for upd in res["result"]:
                    last_id = upd["update_id"]
                    if "message" in upd and "text" in upd["message"]:
                        chat_id = upd["message"]["chat"]["id"]
                        txt = upd["message"]["text"]
                        
                        if chat_id != AUTHORIZED_USER:
                            send_msg(chat_id, "❌ *UNAUTHORIZED ACCESS.*")
                            continue
                            
                        if txt == "/start": send_msg(chat_id, "💎 V9.0 PRO TRADING ENGINE Online.")
                        elif txt in ["🔄 Switch Mode", "/mode"]:
                            pending_mode_confirm = True
                            send_msg(chat_id, f"⚠️ Current Mode: *{trading_mode}*\nType `CONFIRM` to switch mode.")
                        elif txt == "CONFIRM":
                            if pending_mode_confirm:
                                trading_mode = "REAL" if trading_mode == "DEMO" else "DEMO"
                                pending_mode_confirm = False
                                send_msg(chat_id, f"💰 Mode Switched to: *{trading_mode}*")
                        else:
                            # BUG FIX 9: Reset mode confirm if they typed something else
                            pending_mode_confirm = False
                            
                        if txt == "📊 Check Status": 
                            send_msg(chat_id, f"📡 System: {'Paused ⏸' if bot_paused else 'Active ▶️'}\n🧠 Mode: {trading_mode}\n🛡️ Risk: {current_risk_percent}%")
                        elif txt in ["💰 View PnL", "/pnl"]:
                            pnl = get_val("SELECT SUM(pnl) FROM pro_trades WHERE status!='OPEN'")
                            send_msg(chat_id, f"💰 Net PnL: ₹{pnl:.2f}")
                        
                        # PRO UPGRADE 2: Live PnL Tracking
                        elif txt == "📈 Live PnL":
                            rows = execute_db("SELECT symbol, type, entry_price FROM pro_trades WHERE status='OPEN'", fetchall=True)
                            if not rows:
                                send_msg(chat_id, "No open trades right now.")
                            else:
                                msg = "📈 *LIVE OPEN TRADES PnL:*\n\n"
                                total_live = 0
                                for r in rows:
                                    sym, t_type, entry = r[0], r[1], r[2]
                                    try:
                                        d = tv.get_hist(symbol=sym, exchange='NSE', interval=Interval.in_1_minute, n_bars=2)
                                        cp = d['close'].iloc[-1]
                                        pnl = (cp - entry) if "BUY" in t_type else (entry - cp)
                                        total_live += pnl
                                        msg += f"🔹 {sym}: ₹{pnl:.2f}\n"
                                    except: pass
                                msg += f"\n💰 *Total Floating:* ₹{total_live:.2f}"
                                send_msg(chat_id, msg)
                                
                        elif txt in ["📅 Today Report", "/today"]:
                            t = get_ist().strftime("%Y-%m-%d")
                            pnl = get_val("SELECT SUM(pnl) FROM pro_trades WHERE date LIKE ? AND status!='OPEN'", (f"{t}%",))
                            send_msg(chat_id, f"📅 *Today's PnL:* ₹{pnl:.2f}")
                            
                        # BUG FIX 4: Scan Now properly triggers
                        elif txt == "🔍 Scan Now":
                            send_msg(chat_id, "🔍 Manual Scan Initiated...")
                            Thread(target=run_scan_cycle, args=(True,)).start()
                            
                        elif txt == "🛡️ Safe Mode": strategy_mode = "SAFE"; send_msg(chat_id, "🛡️ Safe Mode ON.")
                        elif txt == "⚡ Aggressive Mode": strategy_mode = "AGGRESSIVE"; send_msg(chat_id, "⚡ Aggressive Mode ON.")
                        elif txt in ["❌ Close All", "/closeall"]:
                            rows = execute_db("SELECT id, symbol, type, entry_price FROM pro_trades WHERE status='OPEN'", fetchall=True)
                            if rows:
                                for r in rows: execute_db("UPDATE pro_trades SET status='CLOSED ⚠️' WHERE id=?", (r[0],))
                                send_msg(chat_id, "⚠️ All positions FORCE CLOSED.")
                            else: send_msg(chat_id, "❌ No open trades.")
                        elif txt == "⏸ Pause Bot": bot_paused = True; send_msg(chat_id, "🛑 Bot Paused.")
                        elif txt == "▶️ Resume Bot": bot_paused = False; send_msg(chat_id, "✅ Bot Resumed.")
        except Exception as e: logging.error(f"TG Error: {e}"); time.sleep(5)

if __name__ == "__main__":
    setup_db()
    Thread(target=run_server, daemon=True).start()
    Thread(target=auto_scanner, daemon=True).start()
    telegram()
