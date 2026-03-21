import requests
import time
import pandas as pd
import sqlite3
import os
import logging
import json
from datetime import datetime
from threading import Thread
from tvDatafeed import TvDatafeed, Interval
from flask import Flask

# ==========================================
# 🛡️ SYSTEM FIXES: Logging & Security
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# DANGER FIXED: Token ab Environment Variable se aayega!
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
BASE_URL = f"https://api.telegram.org/bot{TOKEN}" if TOKEN else ""

# ==========================================
# 🌐 DUMMY WEB SERVER (Cloud Keep-Alive)
# ==========================================
app = Flask(__name__)
@app.route('/')
def home():
    return "🔥 V2.0 PRO AI Trading Bot is LIVE & Secured!"

def run_server():
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

# ==========================================
# 🗄️ ADVANCED DB (PnL & Status Tracking)
# ==========================================
def setup_db():
    conn = sqlite3.connect('trades.db')
    cursor = conn.cursor()
    # Nayi table banayi taaki purane data se conflict na ho
    cursor.execute('''CREATE TABLE IF NOT EXISTS pro_trades 
                      (id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, symbol TEXT, type TEXT, 
                       entry_price REAL, sl REAL, tp REAL, trailing_sl REAL, status TEXT, pnl REAL)''')
    conn.commit()
    conn.close()

def save_trade(symbol, trade_type, price, sl, tp, trailing_sl):
    conn = sqlite3.connect('trades.db')
    cursor = conn.cursor()
    date_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute('INSERT INTO pro_trades (date, symbol, type, entry_price, sl, tp, trailing_sl, status, pnl) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', 
                   (date_now, symbol, trade_type, price, sl, tp, trailing_sl, "OPEN", 0.0))
    conn.commit()
    conn.close()

def send_msg(chat_id, text, show_buttons=True):
    if not TOKEN:
        logging.error("Telegram Token missing! Cannot send message.")
        return
        
    url = f"{BASE_URL}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    
    if show_buttons:
        keyboard = {"keyboard": [[{"text": "📊 Check Status"}, {"text": "📜 Show History"}], [{"text": "⏸ Pause Bot"}, {"text": "▶️ Resume Bot"}]], "resize_keyboard": True}
        payload["reply_markup"] = keyboard
        
    requests.post(url, json=payload)

# ==========================================
# 📈 TRADINGVIEW SETUP
# ==========================================
logging.info("TradingView se connect kar raha hoon...")
tv_user = os.getenv("TV_USER")
tv_pass = os.getenv("TV_PASS")
try:
    if tv_user and tv_pass:
        tv = TvDatafeed(username=tv_user, password=tv_pass) # Login (Agar credentials hain)
    else:
        tv = TvDatafeed() # Guest mode fallback
except Exception as e:
    logging.error(f"TradingView Login Failed: {e}")
    tv = TvDatafeed()

bot_paused = False
my_chat_id = None 
last_trade_signals = {"NIFTY": "WAIT", "BANKNIFTY": "WAIT", "CNXFINANCE": "WAIT"}

# ==========================================
# 🧠 SMART MONEY LOGIC & INDICATORS
# ==========================================
def calculate_rsi(data, window=14):
    delta = data['close'].diff() 
    gain = delta.clip(lower=0).ewm(alpha=1/window, adjust=False).mean()
    loss = -delta.clip(upper=0).ewm(alpha=1/window, adjust=False).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calculate_macd(data):
    ema12 = data['close'].ewm(span=12, adjust=False).mean()
    ema26 = data['close'].ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    return macd, signal

# 🛡️ MAIN SCANNER (Runs in a separate thread)
def market_scanner_thread():
    global my_chat_id, bot_paused, last_trade_signals
    
    symbols_to_scan = ['NIFTY', 'BANKNIFTY', 'CNXFINANCE']
    
    while True:
        if my_chat_id is None or bot_paused:
            time.sleep(10)
            continue
            
        for sym in symbols_to_scan:
            try:
                # 1. MTF Confirmation: Fetch 15-Min Trend
                data_15m = tv.get_hist(symbol=sym, exchange='NSE', interval=Interval.in_15_minute, n_bars=100)
                if data_15m is None or data_15m.empty: continue
                trend_15m_ema = data_15m['close'].ewm(span=50, adjust=False).mean().iloc[-1]
                trend_15m = "UP" if data_15m['close'].iloc[-1] > trend_15m_ema else "DOWN"
                
                # 2. Main 5-Min Data
                data = tv.get_hist(symbol=sym, exchange='NSE', interval=Interval.in_5_minute, n_bars=210)
                if data is None or data.empty: continue
                
                # Calculations
                data['EMA_200'] = data['close'].ewm(span=200, adjust=False).mean()
                data['RSI_14'] = calculate_rsi(data)
                macd, macd_signal = calculate_macd(data)
                data['Volume_MA'] = data['volume'].rolling(20).mean() # Volume Confirmation
                
                current_price = data['close'].iloc[-1]
                current_ema = data['EMA_200'].iloc[-1]
                current_rsi = data['RSI_14'].iloc[-1]
                current_macd = macd.iloc[-1]
                current_signal = macd_signal.iloc[-1]
                current_vol = data['volume'].iloc[-1]
                vol_ma = data['Volume_MA'].iloc[-1]
                
                decision = "WAIT 🟡"
                sl, tp, trailing_sl = 0, 0, 0
                
                # Risk Management (Example Balance)
                balance = 50000 
                risk_per_trade = balance * 0.02 # Max 2% loss
                
                # 💡 SMART ENTRY: Check if price is near EMA (Avoid FOMO late entry)
                distance_from_ema = abs(current_price - current_ema) / current_ema * 100
                is_smart_entry = distance_from_ema < 0.25 # Entry valid only if price is within 0.25% of 200 EMA
                
                # TRIPLE CONFIRMATION + MTF + VOLUME
                if current_price > current_ema and current_rsi > 55 and current_macd > current_signal:
                    if trend_15m == "UP" and current_vol > vol_ma and is_smart_entry:
                        decision = "BUY 🟢"
                        sl = current_price - (current_price * 0.002) 
                        tp = current_price + (current_price * 0.005)
                        trailing_sl = current_price - (current_price * 0.001) # Trailing Logic
                
                elif current_price < current_ema and current_rsi < 45 and current_macd < current_signal:
                    if trend_15m == "DOWN" and current_vol > vol_ma and is_smart_entry:
                        decision = "SELL 🔴"
                        sl = current_price + (current_price * 0.002) 
                        tp = current_price - (current_price * 0.005)
                        trailing_sl = current_price + (current_price * 0.001)
                
                # Spam Control & DB Save
                if decision != "WAIT 🟡" and decision != last_trade_signals[sym]:
                    save_trade(sym, decision, current_price, sl, tp, trailing_sl)
                    last_trade_signals[sym] = decision 
                    
                    msg = (f"🚨 *PRO ALERT: {sym}* 🚨\n\n"
                           f"🤖 *Action:* {decision}\n"
                           f"💰 *Risk Amount:* ₹{risk_per_trade:.0f}\n"
                           f"🔸 *Entry:* ₹{current_price:.2f}\n"
                           f"🎯 *Target:* ₹{tp:.2f}\n"
                           f"🛡️ *SL:* ₹{sl:.2f} | *TSL:* ₹{trailing_sl:.2f}\n\n"
                           f"✅ *Checks:* MTF (15m) | Volume Surge | Smart Entry (Near EMA)")
                    send_msg(my_chat_id, msg)
                    
                elif decision == "WAIT 🟡":
                    last_trade_signals[sym] = "WAIT 🟡" 
                    
            except Exception as e:
                logging.error(f"Scan Error on {sym}: {e}")
            
            time.sleep(2) # Prevent TradingView API Ban
            
        time.sleep(60) # Scan every 1 minute

# ==========================================
# 📱 TELEGRAM LISTENER (Main Thread)
# ==========================================
def telegram_listener():
    global my_chat_id, bot_paused
    last_update_id = None
    
    while True:
        try:
            if not TOKEN:
                time.sleep(10)
                continue
                
            url = f"{BASE_URL}/getUpdates?timeout=5"
            if last_update_id:
                url += f"&offset={last_update_id + 1}"
                
            response = requests.get(url).json()
            
            if "result" in response and len(response["result"]) > 0:
                for update in response["result"]:
                    last_update_id = update["update_id"]
                    
                    if "message" in update and "text" in update["message"]:
                        my_chat_id = update["message"]["chat"]["id"] 
                        user_text = update["message"]["text"]
                        
                        if user_text == "/start":
                            send_msg(my_chat_id, "Boss, V2.0 PRO Bot ON hai! 🛡️ Secured & Optimized.")
                        elif user_text == "⏸ Pause Bot":
                            bot_paused = True
                            send_msg(my_chat_id, "🛑 Bot PAUSED. Scanning stopped.")
                        elif user_text == "▶️ Resume Bot":
                            bot_paused = False
                            send_msg(my_chat_id, "✅ Bot RESUMED. Advanced scanning active!")
                        elif user_text == "📊 Check Status":
                            status = "⏸ Paused" if bot_paused else "▶️ Active"
                            send_msg(my_chat_id, f"📡 *Status:* {status}\n👀 *MTF Radar:* Nifty, BankNifty, FinNifty")
                        elif user_text == "📜 Show History":
                            conn = sqlite3.connect('trades.db')
                            c = conn.cursor()
                            c.execute('SELECT date, symbol, type, entry_price FROM pro_trades ORDER BY id DESC LIMIT 3')
                            rows = c.fetchall()
                            conn.close()
                            if rows:
                                hist_msg = "📂 *Aakhiri 3 PRO Trades:*\n\n"
                                for r in rows: hist_msg += f"🗓 {r[0]}\n📈 {r[1]} | {r[2]} @ ₹{r[3]:.2f}\n\n"
                                send_msg(my_chat_id, hist_msg)
                            else:
                                send_msg(my_chat_id, "Abhi tak koi trade nahi mila.")
                                
            time.sleep(1)
        except Exception as e:
            logging.error(f"Telegram Listener Error: {e}")
            time.sleep(5)

# ==========================================
# 🚀 LAUNCH PROTOCOL
# ==========================================
if __name__ == "__main__":
    setup_db()
    Thread(target=run_server, daemon=True).start()
    Thread(target=market_scanner_thread, daemon=True).start()
    logging.info("🚀 V2.0 PRO Bot Launched Successfully!")
    telegram_listener()
