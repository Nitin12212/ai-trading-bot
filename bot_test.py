import requests
import time
import pandas as pd
import sqlite3
import os
import logging
from datetime import datetime, timedelta
from threading import Thread
from tvDatafeed import TvDatafeed, Interval
from flask import Flask

# ==========================================
# 🛡️ 1. SYSTEM & BOT SETUP
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Aapka Telegram Token (Seedha yahan daal diya hai taaki error na aaye)
TOKEN = "7628056199:AAGidOMN4cLJkLKooYaSWfqd5BQcMAnYvk8"
BASE_URL = f"https://api.telegram.org/bot{TOKEN}"

# Cloud Server ko zinda rakhne ke liye Web Server
app = Flask(__name__)
@app.route('/')
def home(): return "🟢 Bot is Successfully Running on Cloud!"

def run_server():
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

# Globals
bot_paused = False
trading_mode = "DEMO" # Ya toh DEMO ya REAL
daily_loss_limit = -2000.0 # Circuit Breaker limit
my_chat_id = None
tv = TvDatafeed()

# ==========================================
# 🗄️ 2. SAFE DATABASE SETUP
# ==========================================
# Database Locking error se bachne ke liye safe function
def execute_db(query, params=(), fetch=False, fetchall=False):
    try:
        conn = sqlite3.connect('trades.db', timeout=10) # Timeout zaroori hai
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

def get_ist(): 
    return datetime.utcnow() + timedelta(hours=5, minutes=30)

# ==========================================
# 📱 3. TELEGRAM MESSAGING & BUTTONS
# ==========================================
def send_msg(chat_id, text):
    if not TOKEN or not chat_id: return
    url = f"{BASE_URL}/sendMessage"
    
    # Har trade aur command ke liye Buttons
    keyboard = {
        "keyboard": [
            [{"text": "📊 Check Status"}, {"text": "📅 Today Report"}],
            [{"text": "💰 View PnL"}, {"text": "📂 Open Trades"}],
            [{"text": "🔄 Switch Mode (Demo/Real)"}, {"text": "❌ Close All"}],
            [{"text": "⏸ Pause Bot"}, {"text": "▶️ Resume Bot"}]
        ],
        "resize_keyboard": True
    }
    
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown", "reply_markup": keyboard}
    try: requests.post(url, json=payload)
    except Exception as e: logging.error(f"Send Msg Error: {e}")

# ==========================================
# 🧠 4. LIVE MARKET SCANNER & LOGIC
# ==========================================
def scanner():
    global my_chat_id, bot_paused, trading_mode
    symbols = ['NIFTY', 'BANKNIFTY', 'CNXFINANCE']
    
    while True:
        if my_chat_id is None or bot_paused:
            time.sleep(5)
            continue
        
        # 🛑 Circuit Breaker Check (Max Loss)
        today = get_ist().strftime("%Y-%m-%d")
        res = execute_db("SELECT SUM(pnl) FROM pro_trades WHERE date LIKE ? AND status!='OPEN'", (f"{today}%",), fetch=True)
        today_pnl = res[0] if res and res[0] else 0.0

        if today_pnl <= daily_loss_limit:
            send_msg(my_chat_id, f"⚠️ *CIRCUIT BREAKER:* Aaj ka loss ₹{today_pnl:.2f} ho gaya hai. System Auto-Locked!")
            bot_paused = True
            time.sleep(60)
            continue
            
        for sym in symbols:
            try:
                # Live Data Fetch (5 Min Candle)
                data = tv.get_hist(symbol=sym, exchange='NSE', interval=Interval.in_5_minute, n_bars=100)
                if data is None or data.empty: continue
                
                cp = data['close'].iloc[-1]
                ema200 = data['close'].ewm(span=200, adjust=False).mean().iloc[-1]
                
                # RSI Calculation
                delta = data['close'].diff()
                gain = delta.clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
                loss = -delta.clip(upper=0).ewm(alpha=1/14, adjust=False).mean()
                rsi = 100 - (100 / (1 + (gain / loss))).iloc[-1]
                
                # 🛡️ Exit Check (Take Profit / Stop Loss)
                open_trades = execute_db("SELECT id, type, entry_price, sl, tp FROM pro_trades WHERE symbol=? AND status='OPEN'", (sym,), fetchall=True)
                if open_trades:
                    for t in open_trades:
                        t_id, t_type, entry, sl, tp = t
                        status, pnl, msg = "OPEN", 0.0, None
                        
                        if t_type == "BUY 🟢":
                            if cp >= tp: status, pnl, msg = "PROFIT ✅", tp - entry, f"🎯 TARGET HIT ({trading_mode}): {sym}\n💰 Profit: ₹{tp - entry:.2f}"
                            elif cp <= sl: status, pnl, msg = "LOSS ❌", sl - entry, f"🛑 SL HIT ({trading_mode}): {sym}\n💸 Loss: ₹{entry - sl:.2f}"
                        elif t_type == "SELL 🔴":
                            if cp <= tp: status, pnl, msg = "PROFIT ✅", entry - tp, f"🎯 TARGET HIT ({trading_mode}): {sym}\n💰 Profit: ₹{entry - tp:.2f}"
                            elif cp >= sl: status, pnl, msg = "LOSS ❌", entry - sl, f"🛑 SL HIT ({trading_mode}): {sym}\n💸 Loss: ₹{sl - entry:.2f}"
                        
                        if status != "OPEN":
                            execute_db("UPDATE pro_trades SET status=?, pnl=? WHERE id=?", (status, pnl, t_id))
                            send_msg(my_chat_id, msg)
                    continue # Agar trade open hai toh naya trade nahi lena
                
                # 🚀 Entry Logic (Live Trade Execution)
                decision = "WAIT"
                if cp > ema200 and rsi > 55: decision = "BUY 🟢"
                elif cp < ema200 and rsi < 45: decision = "SELL 🔴"

                if decision != "WAIT":
                    sl = cp - (cp * 0.002) if "BUY" in decision else cp + (cp * 0.002)
                    tp = cp + (cp * 0.004) if "BUY" in decision else cp - (cp * 0.004)
                    
                    execute_db('INSERT INTO pro_trades (date, symbol, type, entry_price, sl, tp, status, pnl, mode) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                               (get_ist().strftime("%Y-%m-%d %H:%M"), sym, decision, cp, sl, tp, "OPEN", 0.0, trading_mode))
                    
                    send_msg(my_chat_id, f"🚀 *{trading_mode} ENTRY TAKEN:* {sym}\n\n🤖 *Action:* {decision}\n🔸 *Entry:* ₹{cp:.2f}\n🎯 *Target:* ₹{tp:.2f}\n🛡️ *SL:* ₹{sl:.2f}")

            except Exception as e:
                logging.error(f"Scan error on {sym}: {e}")
            time.sleep(2) # TV API ban se bachne ke liye delay
        time.sleep(60) # Har 1 min me scan

# ==========================================
# 🎮 5. TELEGRAM COMMAND LISTENER
# ==========================================
def telegram():
    global my_chat_id, bot_paused, trading_mode
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
                        
                        # Command Actions
                        if txt == "/start": send_msg(my_chat_id, "✅ *System Online!* AI is now active and scanning.")
                        elif txt == "🔄 Switch Mode (Demo/Real)":
                            trading_mode = "REAL" if trading_mode == "DEMO" else "DEMO"
                            send_msg(my_chat_id, f"🔄 System Mode Switched to: *{trading_mode.upper()}*")
                        elif txt == "📊 Check Status":
                            send_msg(my_chat_id, f"📡 Status: {'Paused ⏸' if bot_paused else 'Active ▶️'}\n🧠 Trading Mode: {trading_mode}")
                        elif txt == "💰 View PnL":
                            res = execute_db("SELECT SUM(pnl) FROM pro_trades", fetch=True)
                            pnl = res[0] if res and res[0] else 0.0
                            send_msg(my_chat_id, f"💰 Net PnL: ₹{pnl:.2f}")
                        elif txt == "📅 Today Report":
                            today = get_ist().strftime("%Y-%m-%d")
                            res = execute_db("SELECT SUM(pnl) FROM pro_trades WHERE date LIKE ?", (f"{today}%",), fetch=True)
                            pnl = res[0] if res and res[0] else 0.0
                            send_msg(my_chat_id, f"📅 Aaj ka PnL: ₹{pnl:.2f}")
                        elif txt == "📂 Open Trades":
                            rows = execute_db("SELECT symbol, type, entry_price FROM pro_trades WHERE status='OPEN'", fetchall=True)
                            if rows:
                                msg = "⚡ *LIVE OPEN TRADES:*\n\n"
                                for r in rows: msg += f"🔹 {r[0]} | {r[1]} @ ₹{r[2]:.2f}\n"
                                send_msg(my_chat_id, msg)
                            else: send_msg(my_chat_id, "Abhi koi trade open nahi hai.")
                        elif txt == "❌ Close All":
                            rows = execute_db("SELECT id, symbol, type, entry_price FROM pro_trades WHERE status='OPEN'", fetchall=True)
                            if rows:
                                for r in rows:
                                    t_id, sym, t_type, entry = r
                                    try:
                                        d = tv.get_hist(symbol=sym, exchange='NSE', interval=Interval.in_1_minute, n_bars=2)
                                        cp = d['close'].iloc[-1]
                                        pnl = (cp - entry) if t_type == "BUY 🟢" else (entry - cp)
                                        execute_db("UPDATE pro_trades SET status='CLOSED ⚠️', pnl=? WHERE id=?", (pnl, t_id))
                                        send_msg(my_chat_id, f"⚠️ {sym} Force Closed @ ₹{cp:.2f}\n💸 PnL: ₹{pnl:.2f}")
                                    except: pass
                            else: send_msg(my_chat_id, "❌ Koi open trade nahi hai.")
                        elif txt == "⏸ Pause Bot":
                            bot_paused = True
                            send_msg(my_chat_id, "🛑 Bot Paused. Trading Roki gayi hai.")
                        elif txt == "▶️ Resume Bot":
                            bot_paused = False
                            send_msg(my_chat_id, "✅ Bot Resumed. Scanning chalu!")
        except Exception as e:
            logging.error(f"Telegram Listener Error: {e}")
            time.sleep(5)
        time.sleep(1)

# ==========================================
# 🚀 6. RUN THE SYSTEM
# ==========================================
if __name__ == "__main__":
    setup_db()
    Thread(target=run_server, daemon=True).start()
    Thread(target=scanner, daemon=True).start()
    telegram()
