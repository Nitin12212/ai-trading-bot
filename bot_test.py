import requests
import time
import pandas as pd
import sqlite3
from datetime import datetime
from tvDatafeed import TvDatafeed, Interval
from flask import Flask
from threading import Thread
import os

# ==========================================
# 🌐 DUMMY WEB SERVER (Cloud ko khush rakhne ke liye)
# ==========================================
app = Flask(__name__)

@app.route('/')
def home():
    return "🤖 AI Trading Bot is Alive and Running on Cloud!"

def run_server():
    # Render khud ek PORT deta hai, nahi toh 8080 use karega
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

# Web server ko background (alag dhaage) mein start karna
Thread(target=run_server).start()

# ==========================================
# 🤖 BOT KA ASLI CODE YAHAN SE HAI
# ==========================================
# 👇 Yahan Apna Token Zaroor Daalein!
TOKEN = "7628056199:AAGidOMN4cLJkLKooYaSWfqd5BQcMAnYvk8"
BASE_URL = f"https://api.telegram.org/bot{TOKEN}"

def setup_db():
    conn = sqlite3.connect('trades.db')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS demo_trades 
                      (id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, type TEXT, price REAL, reason TEXT)''')
    conn.commit()
    conn.close()

def save_trade(trade_type, price, reason):
    conn = sqlite3.connect('trades.db')
    cursor = conn.cursor()
    date_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute('INSERT INTO demo_trades (date, type, price, reason) VALUES (?, ?, ?, ?)', 
                   (date_now, trade_type, price, reason))
    conn.commit()
    conn.close()

print("🔄 TradingView se connect kar raha hoon...")
tv = TvDatafeed() 

def calculate_rsi(data, window=14):
    delta = data['close'].diff() 
    gain = delta.clip(lower=0).ewm(alpha=1/window, adjust=False).mean()
    loss = -delta.clip(upper=0).ewm(alpha=1/window, adjust=False).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

last_trade_signal = "WAIT 🟡"
my_chat_id = None 

def auto_scan_market():
    global last_trade_signal, my_chat_id
    if my_chat_id is None:
        return 

    try:
        data = tv.get_hist(symbol='NIFTY', exchange='NSE', interval=Interval.in_5_minute, n_bars=210)
        if data is None or data.empty:
            return
            
        data['EMA_200'] = data['close'].ewm(span=200, adjust=False).mean()
        data['RSI_14'] = calculate_rsi(data)
        
        current_price = data['close'].iloc[-1]
        current_ema = data['EMA_200'].iloc[-1]
        current_rsi = data['RSI_14'].iloc[-1]
        
        decision = "WAIT 🟡"
        reason = ""
        
        if current_price > current_ema and 40 < current_rsi < 60:
            decision = "BUY 🟢"
            reason = "Uptrend and Good RSI"
        elif current_price < current_ema and current_rsi < 40:
            decision = "SELL 🔴"
            reason = "Downtrend and Weak RSI"
            
        if decision != "WAIT 🟡" and decision != last_trade_signal:
            save_trade(decision, current_price, reason)
            last_trade_signal = decision 
            
            msg = (f"🚨 *AUTO ALERT: Naya Trade Mila!* 🚨\n\n"
                   f"🔸 Nifty Live Price: ₹{current_price:.2f}\n"
                   f"🔸 EMA: ₹{current_ema:.2f} | RSI: {current_rsi:.2f}\n\n"
                   f"🤖 *Action:* {decision}\n"
                   f"💾 Saved to Database.")
            requests.get(f"{BASE_URL}/sendMessage?chat_id={my_chat_id}&text={msg}")
            
        elif decision == "WAIT 🟡":
            last_trade_signal = "WAIT 🟡" 
            
    except Exception as e:
        print(f"Scan Error: {e}")

setup_db()
print("🤖 Auto-Pilot Bot + Web Server ON ho gaya hai!")

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
                    
                    if user_text.lower() == "/start":
                        requests.get(f"{BASE_URL}/sendMessage?chat_id={my_chat_id}&text='Boss, Cloud Auto-Pilot ON hai! ☁️✈️'")
                    
        current_time = time.time()
        if current_time - last_scan_time > 60: 
            if my_chat_id is not None:
                auto_scan_market()
            last_scan_time = current_time
            
        time.sleep(1)
    except Exception as e:
        time.sleep(5)