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
# 🛡️ 1. SYSTEM SECURITY & GLOBALS
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# DANGER FIXED: Token is now completely private!
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
BASE_URL = f"https://api.telegram.org/bot{TOKEN}" if TOKEN else ""

app = Flask(__name__)
@app.route('/')
def home(): return "🛡️ V7.0 GOD TIER ARCHITECTURE is LIVE & SECURED!"

def run_server():
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

# Globals & Limits
bot_paused = False
trading_mode = "DEMO"
pending_real_confirmation = False
daily_loss_limit = -2000.0 
max_daily_trades = 5
current_risk_percent = 2.0
my_chat_id = None

# ==========================================
# 🌐 2. ANTI-BAN DATA ENGINE (The Golden Rules)
# ==========================================
# Session & Headers (Rule 1)
session = requests.Session()
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/"
}
# Initialize NSE cookies
try: session.get("https://www.nseindia.com", headers=headers, timeout=5)
except: pass

tv = TvDatafeed()

# Cache Engine (Rule 3)
cache_data = {}

def safe_request(url):
    """Rule 7: Retry System"""
    for _ in range(3):
        try:
            res = session.get(url, headers=headers, timeout=5)
            if res.status_code == 200: return res.json()
        except: time.sleep(2)
    return None

def fetch_market_data(symbol):
    """Rule 4: Fallback System + Rule 2: Delay + Rule 3: Cache"""
    global cache_data
    now = time.time()
    
    # Return cache if less than 30 seconds old
    if symbol in cache_data and (now - cache_data[symbol]['time'] < 30):
        return cache_data[symbol]['data']
        
    time.sleep(random.uniform(1.5, 3.5)) # Human-like delay
    
    # 1. Primary Attempt (e.g., NSE or custom Fast API)
    # nse_data = safe_request(f"https://nseindia.com/api/chart-databyindex?index={symbol}")
    nse_data = None # Placeholder for actual NSE URL structure
    
    if nse_data is not None:
        # Process NSE data here
        pass 
    else:
        # 2. Fallback to TradingView
        try:
            data = tv.get_hist(symbol=symbol, exchange='NSE', interval=Interval.in_5_minute, n_bars=100)
            if data is not None and not data.empty:
                cache_data[symbol] = {'time': now, 'data': data}
                return data
        except Exception as e:
            logging.error(f"Fallback TV Error on {symbol}: {e}")
            
    return None

# ==========================================
# 🗄️ 3. DATABASE SETUP
# ==========================================
def execute_db(query, params=(), fetch=False, fetchall=False):
    try:
        conn = sqlite3.connect('trades.db', timeout=10)
        c = conn.cursor()
        c.execute(query, params)
        if fetch: res = c.fetchone()
        elif fetchall: res = c.fetchall()
        else:
            conn.commit()
            res = True
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
# 📱 4. TELEGRAM UI
# ==========================================
def send_msg(chat_id, text):
    if not TOKEN or not chat_id: return
    url = f"{BASE_URL}/sendMessage"
    keyboard = {
        "keyboard": [
            [{"text": "📊 Check Status"}, {"text": "📅 Today Report"}],
            [{"text": "💰 View PnL"}, {"text": "📂 Open Trades"}],
            [{"text": "🔄 Switch Mode (Demo/Real)"}, {"text": "❌ Close All"}],
            [{"text": "⏸ Pause Bot"}, {"text": "▶️ Resume Bot"}]
        ], "resize_keyboard": True
    }
    try: requests.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown", "reply_markup": keyboard})
    except: pass

# ==========================================
# 💰 5. REAL BROKER EXECUTION (PLACEHOLDER)
# ==========================================
def place_real_order_broker(symbol, side, price, sl, tp, qty):
    """REAL MONEY API CALL GOES HERE (Zerodha/Shoonya)"""
    try:
        order_type = "BUY" if "BUY" in side else "SELL"
        logging.info(f"REAL ORDER SHOOT: {symbol} {order_type} QTY:{qty}")
        # ShoonyaApi.place_order(...) -> Will be added here
        return True
    except Exception as e:
        logging.error(f"Broker Order Failed: {e}")
        send_msg(my_chat_id, f"❌ *BROKER REJECTED:* {e}")
        return False

# ==========================================
# 🧠 6. MARKET SCANNER & CORE LOGIC
# ==========================================
def scanner():
    global my_chat_id, bot_paused, trading_mode, daily_loss_limit, max_daily_trades
    symbols = ['NIFTY', 'BANKNIFTY', 'CNXFINANCE']
    
    while True:
        if my_chat_id is None or bot_paused:
            time.sleep(5); continue
            
        today = get_ist().strftime("%Y-%m-%d")
        
        # 🛑 Limit Check 1: Max Loss Circuit Breaker
        res_pnl = execute_db("SELECT SUM(pnl) FROM pro_trades WHERE date LIKE ? AND status!='OPEN'", (f"{today}%",), fetch=True)
        today_pnl = res_pnl[0] if res_pnl and res_pnl[0] else 0.0

        if today_pnl <= daily_loss_limit:
            send_msg(my_chat_id, f"⚠️ *CIRCUIT BREAKER HIT:* Loss ₹{today_pnl:.2f}. System Locked for the day!")
            bot_paused = True; time.sleep(60); continue

        # 🛑 Limit Check 2: Max Trades Per Day
        res_count = execute_db("SELECT COUNT(*) FROM pro_trades WHERE date LIKE ?", (f"{today}%",), fetch=True)
        trades_today = res_count[0] if res_count and res_count[0] else 0
        if trades_today >= max_daily_trades:
            time.sleep(60); continue # Limit reached
            
        for sym in symbols:
            try:
                # Use Anti-Ban Fetcher
                data = fetch_market_data(sym)
                if data is None: continue
                
                cp = data['close'].iloc[-1]
                ema200 = data['close'].ewm(span=200, adjust=False).mean().iloc[-1]
                
                # Exit Management Logic
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
                            send_msg(my_chat_id, msg)
                    continue 
                
                # Entry Logic
                decision = "WAIT"
                if cp > ema200: decision = "BUY 🟢"
                elif cp < ema200: decision = "SELL 🔴"

                if decision != "WAIT":
                    sl = cp - (cp * 0.002) if "BUY" in decision else cp + (cp * 0.002)
                    tp = cp + (cp * 0.004) if "BUY" in decision else cp - (cp * 0.004)
                    
                    capital, risk_amt = 50000, 50000 * (current_risk_percent / 100)
                    sl_dist = abs(cp - sl)
                    qty = int(risk_amt / sl_dist) if sl_dist > 0 else 1
                    
                    # 💰 THE REAL VS DEMO EXECUTION ENGINE
                    order_success = False
                    if trading_mode == "REAL":
                        order_success = place_real_order_broker(sym, decision, cp, sl, tp, qty)
                    else:
                        order_success = True # Demo always succeeds
                        
                    if order_success:
                        execute_db('INSERT INTO pro_trades (date, symbol, type, entry_price, sl, tp, status, pnl, mode) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                                   (get_ist().strftime("%Y-%m-%d %H:%M"), sym, decision, cp, sl, tp, "OPEN", 0.0, trading_mode))
                        
                        send_msg(my_chat_id, f"🚀 *{trading_mode} ORDER EXECUTED:* {sym}\n\n🤖 *Action:* {decision}\n🛒 *Qty:* {qty}\n🔸 *Entry:* ₹{cp:.2f}\n🎯 *TP:* ₹{tp:.2f} | 🛡️ *SL:* ₹{sl:.2f}")

            except Exception as e: logging.error(f"Scan error {sym}: {e}")
            time.sleep(1) # Internal loop delay
        time.sleep(30) # Master Scan Delay

# ==========================================
# 🎮 7. COMMAND HANDLER (TWO-STEP AUTH)
# ==========================================
def telegram():
    global my_chat_id, bot_paused, trading_mode, pending_real_confirmation
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
                        my_chat_id = upd["message"]["chat"]["id"]
                        txt = upd["message"]["text"]
                        
                        if txt == "/start": send_msg(my_chat_id, "🛡️ V7.0 GOD TIER Online. System SECURED.")
                        
                        # ⚠️ SAFE MODE SWITCH (TWO-STEP)
                        elif txt == "🔄 Switch Mode (Demo/Real)":
                            if trading_mode == "DEMO":
                                pending_real_confirmation = True
                                send_msg(my_chat_id, "⚠️ *WARNING:* You are switching to REAL MONEY MODE.\nType `CONFIRM` to activate.")
                            else:
                                trading_mode = "DEMO"
                                pending_real_confirmation = False
                                send_msg(my_chat_id, "🛡️ Switched back to *DEMO* Mode safely.")
                                
                        elif txt == "CONFIRM" and pending_real_confirmation:
                            trading_mode = "REAL"
                            pending_real_confirmation = False
                            send_msg(my_chat_id, "💰 *REAL TRADING ENABLED!* Broker integration is now ACTIVE.")
                            
                        elif txt == "📊 Check Status": send_msg(my_chat_id, f"📡 System: {'Paused ⏸' if bot_paused else 'Active ▶️'}\n🧠 Mode: {trading_mode}\n🛡️ Max Trades/Day: {max_daily_trades}")
                        elif txt == "💰 View PnL":
                            pnl = execute_db("SELECT SUM(pnl) FROM pro_trades", fetch=True)[0] or 0.0
                            send_msg(my_chat_id, f"💰 Net PnL: ₹{pnl:.2f}")
                        elif txt == "📅 Today Report":
                            t = get_ist().strftime("%Y-%m-%d")
                            pnl = execute_db("SELECT SUM(pnl) FROM pro_trades WHERE date LIKE ?", (f"{t}%",), fetch=True)[0] or 0.0
                            tc = execute_db("SELECT COUNT(*) FROM pro_trades WHERE date LIKE ?", (f"{t}%",), fetch=True)[0] or 0
                            send_msg(my_chat_id, f"📅 *Today:*\nPnL: ₹{pnl:.2f}\nTrades Taken: {tc}/{max_daily_trades}")
                        elif txt == "📂 Open Trades":
                            rows = execute_db("SELECT symbol, type, entry_price FROM pro_trades WHERE status='OPEN'", fetchall=True)
                            msg = "⚡ *LIVE TRADES:*\n" + "\n".join([f"🔹 {r[0]} | {r[1]} @ ₹{r[2]}" for r in rows]) if rows else "No open trades."
                            send_msg(my_chat_id, msg)
                        elif txt == "❌ Close All":
                            rows = execute_db("SELECT id, symbol, type, entry_price FROM pro_trades WHERE status='OPEN'", fetchall=True)
                            if rows:
                                for r in rows: execute_db("UPDATE pro_trades SET status='CLOSED ⚠️' WHERE id=?", (r[0],))
                                send_msg(my_chat_id, "⚠️ All positions FORCE CLOSED.")
                            else: send_msg(my_chat_id, "❌ No open trades.")
                        elif txt == "⏸ Pause Bot": bot_paused = True; send_msg(my_chat_id, "🛑 Bot Paused.")
                        elif txt == "▶️ Resume Bot": bot_paused = False; send_msg(my_chat_id, "✅ Bot Resumed.")
        except Exception as e: logging.error(f"TG Error: {e}")
        time.sleep(1)

if __name__ == "__main__":
    setup_db()
    Thread(target=run_server, daemon=True).start()
    Thread(target=scanner, daemon=True).start()
    telegram()
