import requests
import time
import pandas as pd
import sqlite3
from datetime import datetime
from tvDatafeed import TvDatafeed, Interval
from flask import Flask
import os
from threading import Thread

# ==========================================
# 🌐 DUMMY WEB SERVER (Cloud ke liye)
# ==========================================
app = Flask(__name__)
@app.route('/')
def home():
    return "🚀 Ultimate AI Trading Bot is LIVE on Cloud!"

def run_server():
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

Thread(target=run_server).start()

# ==========================================
# 🤖 BOT KA ASLI CODE
# ==========================================
TOKEN = "7628056199:AAGidOMN4cLJkLKooYaSWfqd5BQcMAnYvk8"
BASE_URL = f"https://api.telegram.org/bot{TOKEN}"

bot_paused = False # Power 4: Bot ko rokne ka switch

def setup_db():
    conn = sqlite3.connect('trades.db')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS demo_trades 
                      (id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, symbol TEXT, type TEXT, price REAL, sl REAL, tp REAL)''')
    conn.commit()
    conn.close()

def save_trade(symbol, trade_type, price, sl, tp):
    conn = sqlite3.connect('trades.db')
    cursor = conn.cursor()
    date_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute('INSERT INTO demo_trades (date, symbol, type, price, sl, tp) VALUES (?, ?, ?, ?, ?, ?)', 
                   (date_now, symbol, trade_type, price, sl, tp))
    conn.commit()
    conn.close()

# Power 4: Telegram Buttons bhejne ka function (BUG FIXED HERE)
def send_msg(chat_id, text, show_buttons=True):
    url = f"{BASE_URL}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    
    if show_buttons:
        keyboard = {
            "keyboard": [
                [{"text": "📊 Check Status"}, {"text": "📜 Show History"}],
                [{"text": "⏸ Pause Bot"}, {"text": "▶️ Resume Bot"}]
            ],
            "resize_keyboard": True
        }
        # Yahan problem thi, ab theek ho gayi hai!
        payload["reply_markup"] = keyboard
        
    requests.post(url, json=payload)

print("🔄 TradingView se connect kar raha hoon...")
tv = TvDatafeed() 

# Power 1: Dimaag Tez Karna (RSI aur MACD)
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

last_trade_signals = {"NIFTY": "WAIT", "BANKNIFTY": "WAIT", "CNXFINANCE": "WAIT"}
my_chat_id = None 

# Power 2: Multi-Index Radar
def auto_scan_market():
    global my_chat_id, bot_paused
    if my_chat_id is None or bot_paused:
        return 

    symbols_to_scan = ['NIFTY', 'BANKNIFTY', 'CNXFINANCE']
    
    for sym in symbols_to_scan:
        try:
            data = tv.get_hist(symbol=sym, exchange='NSE', interval=Interval.in_5_minute, n_bars=210)
            if data is None or data.empty:
                continue
                
            data['EMA_200'] = data['close'].ewm(span=200, adjust=False).mean()
            data['RSI_14'] = calculate_rsi(data)
            macd, macd_signal = calculate_macd(data)
            
            current_price = data['close'].iloc[-1]
            current_ema = data['EMA_200'].iloc[-1]
            current_rsi = data['RSI_14'].iloc[-1]
            current_macd = macd.iloc[-1]
            current_signal = macd_signal.iloc[-1]
            
            decision = "WAIT 🟡"
            sl, tp = 0, 0
            
            # TRIPLE CONFIRMATION LOGIC
            if current_price > current_ema and current_rsi > 50 and current_macd > current_signal:
                decision = "BUY 🟢"
                sl = current_price - (current_price * 0.002) 
                tp = current_price + (current_price * 0.004) 
                
            elif current_price < current_ema and current_rsi < 50 and current_macd < current_signal:
                decision = "SELL 🔴"
                sl = current_price + (current_price * 0.002) 
                tp = current_price - (current_price * 0.004)
                
            if decision != "WAIT 🟡" and decision != last_trade_signals[sym]:
                save_trade(sym, decision, current_price, sl, tp)
                last_trade_signals[sym] = decision 
                
                msg = (f"🚨 *ULTIMATE ALERT: {sym}* 🚨\n\n"
                       f"🤖 *Action:* {decision}\n"
                       f"🔸 *Entry Price:* ₹{current_price:.2f}\n"
                       f"🎯 *Target (TP):* ₹{tp:.2f}\n"
                       f"🛡️ *Stop-Loss (SL):* ₹{sl:.2f}\n\n"
                       f"📊 *Confirmations:* EMA ✅ | RSI ✅ | MACD ✅\n"
                       f"💾 Saved to Database.")
                send_msg(my_chat_id, msg)
                
            elif decision == "WAIT 🟡":
                last_trade_signals[sym] = "WAIT 🟡" 
                
        except Exception as e:
            print(f"Scan Error on {sym}: {e}")

setup_db()
print("🚀 ULTIMATE Bot ON ho gaya hai!")

last_update_id = None
last_scan_time = 0

while True:
    try:
        url = f"{BASE_URL}/getUpdates?timeout=2"
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
                        send_msg(my_chat_id, "Boss, Ultimate Bot Cloud par Zinda hai! ☁️🔥 Niche diye gaye buttons se control karein.")
                        
                    elif user_text == "⏸ Pause Bot":
                        bot_paused = True
                        send_msg(my_chat_id, "🛑 Bot ko PAUSE kar diya gaya hai. Ab main scan nahi karunga.")
                        
                    elif user_text == "▶️ Resume Bot":
                        bot_paused = False
                        send_msg(my_chat_id, "✅ Bot RESUME ho gaya hai! Scanning shuru...")
                        
                    elif user_text == "📊 Check Status":
                        status = "⏸ Paused" if bot_paused else "▶️ Active & Scanning"
                        send_msg(my_chat_id, f"📡 *Bot Status:* {status}\n👀 *Radar:* Nifty, BankNifty, FinNifty")
                        
                    elif user_text == "📜 Show History":
                        conn = sqlite3.connect('trades.db')
                        c = conn.cursor()
                        c.execute('SELECT date, symbol, type, price FROM demo_trades ORDER BY id DESC LIMIT 3')
                        rows = c.fetchall()
                        conn.close()
                        if rows:
                            hist_msg = "📂 *Aakhiri 3 Trades:*\n\n"
                            for r in rows:
                                hist_msg += f"🗓 {r[0]}\n📈 {r[1]} | {r[2]} @ ₹{r[3]:.2f}\n\n"
                            send_msg(my_chat_id, hist_msg)
                        else:
                            send_msg(my_chat_id, "Abhi tak koi trade nahi liya hai.")
                    
        current_time = time.time()
        if current_time - last_scan_time > 60: 
            if my_chat_id is not None:
                auto_scan_market()
            last_scan_time = current_time
            
        time.sleep(1)
    except Exception as e:
        time.sleep(5)
