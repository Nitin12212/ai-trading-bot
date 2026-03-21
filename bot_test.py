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
# 🛡️ SYSTEM SETUP & GLOBALS
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
BASE_URL = f"https://api.telegram.org/bot{TOKEN}" if TOKEN else ""

app = Flask(__name__)
@app.route('/')
def home(): return "👑 V5.0 ULTIMATE TERMINAL is LIVE!"

def run_server():
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

# GLOBAL SETTINGS
bot_paused = False
alerts_muted = False
strategy_mode = "SAFE" # Options: SAFE, AGGRESSIVE
current_risk_percent = 2.0 
my_chat_id = None 
last_trade_signals = {"NIFTY": "WAIT", "BANKNIFTY": "WAIT", "CNXFINANCE": "WAIT"}
tv = TvDatafeed() 

# ==========================================
# 🗄️ DATABASE
# ==========================================
def setup_db():
    conn = sqlite3.connect('trades.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS pro_trades 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, symbol TEXT, type TEXT, 
                  entry_price REAL, sl REAL, tp REAL, trailing_sl REAL, status TEXT, pnl REAL)''')
    conn.commit()
    conn.close()

def get_ist_time():
    return datetime.utcnow() + timedelta(hours=5, minutes=30)

# ==========================================
# 📱 TELEGRAM UI (ULTIMATE KEYBOARD)
# ==========================================
def send_msg(chat_id, text, show_buttons=True):
    if not TOKEN: return
    url = f"{BASE_URL}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    
    if show_buttons:
        keyboard = {
            "keyboard": [
                [{"text": "📊 Check Status"}, {"text": "📜 Show History"}],
                [{"text": "💰 View PnL"}, {"text": "📊 Win Rate"}],
                [{"text": "📂 Open Trades"}, {"text": "📉 Market Trend"}],
                [{"text": "📅 Today Report"}, {"text": "📉 Drawdown"}],
                [{"text": "🔕 Mute Alerts"}, {"text": "🔔 Unmute Alerts"}],
                [{"text": "⏸ Pause Bot"}, {"text": "▶️ Resume Bot"}],
                [{"text": "⚙️ Set Risk"}, {"text": "❌ Close All"}]
            ],
            "resize_keyboard": True
        }
        payload["reply_markup"] = keyboard
    requests.post(url, json=payload)

# ==========================================
# 🛡️ TRADE MANAGEMENT
# ==========================================
def emergency_close_all():
    global my_chat_id
    conn = sqlite3.connect('trades.db')
    c = conn.cursor()
    c.execute("SELECT id, symbol, type, entry_price FROM pro_trades WHERE status='OPEN'")
    open_trades = c.fetchall()
    
    if not open_trades:
        send_msg(my_chat_id, "⚠️ Koi open trade nahi hai.")
        conn.close()
        return

    msg = "🚨 *EMERGENCY CLOSE TRIGGERED* 🚨\n\n"
    for t in open_trades:
        t_id, sym, t_type, entry = t
        try:
            data = tv.get_hist(symbol=sym, exchange='NSE', interval=Interval.in_1_minute, n_bars=2)
            if data is None or data.empty: continue
            c_price = data['close'].iloc[-1]
            pnl = (c_price - entry) if t_type == "BUY 🟢" else (entry - c_price)
            c.execute("UPDATE pro_trades SET status=?, pnl=? WHERE id=?", ("CLOSED ⚠️", pnl, t_id))
            msg += f"🔹 {sym}: Exit @ ₹{c_price:.2f} | PnL: ₹{pnl:.2f}\n"
        except Exception as e:
            logging.error(f"Error closing {sym}: {e}")
            
    conn.commit()
    conn.close()
    send_msg(my_chat_id, msg)

def manage_open_trades(sym, current_price):
    global my_chat_id, alerts_muted
    if not my_chat_id: return
    
    conn = sqlite3.connect('trades.db')
    c = conn.cursor()
    c.execute("SELECT id, type, entry_price, sl, tp, trailing_sl FROM pro_trades WHERE symbol=? AND status='OPEN'", (sym,))
    trades = c.fetchall()

    for t in trades:
        t_id, t_type, entry, sl, tp, tsl = t
        status, pnl, msg = "OPEN", 0.0, None
        new_tsl = tsl

        if t_type == "BUY 🟢":
            if current_price >= tp: status, pnl, msg = "PROFIT ✅", tp - entry, f"🎯 *TARGET HIT: {sym}*\n💰 Profit: ₹{pnl:.2f}"
            elif current_price <= sl: status, pnl, msg = "LOSS ❌", sl - entry, f"🛑 *SL HIT: {sym}*\n💸 Loss: ₹{pnl:.2f}"
            elif current_price <= tsl and tsl > sl: status, pnl, msg = "TSL HIT ⚠️", tsl - entry, f"⚠️ *TSL HIT: {sym}*\n💸 PnL: ₹{pnl:.2f}"
            elif current_price > entry: new_tsl = max(tsl, current_price - (current_price * 0.001))

        elif t_type == "SELL 🔴":
            if current_price <= tp: status, pnl, msg = "PROFIT ✅", entry - tp, f"🎯 *TARGET HIT: {sym}*\n💰 Profit: ₹{pnl:.2f}"
            elif current_price >= sl: status, pnl, msg = "LOSS ❌", entry - sl, f"🛑 *SL HIT: {sym}*\n💸 Loss: ₹{pnl:.2f}"
            elif current_price >= tsl and tsl < sl: status, pnl, msg = "TSL HIT ⚠️", entry - tsl, f"⚠️ *TSL HIT: {sym}*\n💸 PnL: ₹{pnl:.2f}"
            elif current_price < entry: new_tsl = min(tsl, current_price + (current_price * 0.001))

        c.execute("UPDATE pro_trades SET status=?, pnl=?, trailing_sl=? WHERE id=?", (status, pnl, new_tsl, t_id))
        if msg and not alerts_muted: send_msg(my_chat_id, msg)

    conn.commit()
    conn.close()

# ==========================================
# 🧠 SMART SCANNER & STRATEGY ENGINE
# ==========================================
def calc_rsi(data):
    delta = data['close'].diff()
    gain = delta.clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
    loss = -delta.clip(upper=0).ewm(alpha=1/14, adjust=False).mean()
    return 100 - (100 / (1 + (gain / loss)))

def calc_macd(data):
    ema12 = data['close'].ewm(span=12, adjust=False).mean()
    ema26 = data['close'].ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    return macd, macd.ewm(span=9, adjust=False).mean()

def market_scanner_thread():
    global my_chat_id, bot_paused, last_trade_signals, strategy_mode, alerts_muted, current_risk_percent
    symbols = ['NIFTY', 'BANKNIFTY', 'CNXFINANCE']
    
    while True:
        if my_chat_id is None or bot_paused:
            time.sleep(10)
            continue
            
        now_time = get_ist_time().time()
        start_mkt = datetime.strptime("09:15", "%H:%M").time()
        end_mkt = datetime.strptime("15:30", "%H:%M").time()
        
        # Uncomment below line for live market filtering
        # if not (start_mkt <= now_time <= end_mkt): { time.sleep(60); continue }

        buy_rsi = 60 if strategy_mode == "SAFE" else 50
        sell_rsi = 40 if strategy_mode == "SAFE" else 50

        for sym in symbols:
            try:
                data = tv.get_hist(symbol=sym, exchange='NSE', interval=Interval.in_5_minute, n_bars=210)
                if data is None or data.empty: continue
                
                data['EMA_200'] = data['close'].ewm(span=200, adjust=False).mean()
                data['RSI'] = calc_rsi(data)
                macd, macd_sig = calc_macd(data)
                
                c_price = data['close'].iloc[-1]
                c_ema = data['EMA_200'].iloc[-1]
                
                manage_open_trades(sym, c_price)
                
                conn = sqlite3.connect('trades.db')
                c = conn.cursor()
                c.execute("SELECT COUNT(*) FROM pro_trades WHERE symbol=? AND status='OPEN'", (sym,))
                if c.fetchone()[0] > 0: 
                    conn.close()
                    continue 
                conn.close()
                
                decision = "WAIT 🟡"
                sl, tp, tsl = 0, 0, 0
                
                if c_price > c_ema and data['RSI'].iloc[-1] > buy_rsi and macd.iloc[-1] > macd_sig.iloc[-1]:
                    decision, sl, tp, tsl = "BUY 🟢", c_price - (c_price * 0.002), c_price + (c_price * 0.005), c_price - (c_price * 0.001)
                elif c_price < c_ema and data['RSI'].iloc[-1] < sell_rsi and macd.iloc[-1] < macd_sig.iloc[-1]:
                    decision, sl, tp, tsl = "SELL 🔴", c_price + (c_price * 0.002), c_price - (c_price * 0.005), c_price + (c_price * 0.001)
                
                if decision != "WAIT 🟡" and decision != last_trade_signals[sym]:
                    conn = sqlite3.connect('trades.db')
                    conn.execute('INSERT INTO pro_trades (date, symbol, type, entry_price, sl, tp, trailing_sl, status, pnl) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', 
                               (get_ist_time().strftime("%Y-%m-%d %H:%M"), sym, decision, c_price, sl, tp, tsl, "OPEN", 0.0))
                    conn.commit()
                    conn.close()
                    last_trade_signals[sym] = decision 
                    
                    capital = 50000 
                    risk_amt = capital * (current_risk_percent / 100)
                    sl_dist = abs(c_price - sl)
                    qty = int(risk_amt / sl_dist) if sl_dist > 0 else 0
                    
                    if not alerts_muted:
                        msg = (f"🚨 *{strategy_mode} ENTRY: {sym}* 🚨\n\n🤖 *Action:* {decision}\n🛒 *Qty:* {qty} (Risk {current_risk_percent}%)\n🔸 *Entry:* ₹{c_price:.2f}\n🎯 *Target:* ₹{tp:.2f}\n🛡️ *SL:* ₹{sl:.2f}")
                        send_msg(my_chat_id, msg)
                    
                elif decision == "WAIT 🟡": last_trade_signals[sym] = "WAIT 🟡" 
                    
            except Exception as e: logging.error(f"Scan Error on {sym}: {e}")
            time.sleep(2) 
        time.sleep(60) 

# ==========================================
# 📱 TELEGRAM LISTENER (COMMANDS + BUTTONS)
# ==========================================
def telegram_listener():
    global my_chat_id, bot_paused, current_risk_percent, alerts_muted, strategy_mode
    last_update_id = None
    
    while True:
        try:
            if not TOKEN:
                time.sleep(5)
                continue
                
            url = f"{BASE_URL}/getUpdates?timeout=5"
            if last_update_id: url += f"&offset={last_update_id + 1}"
            response = requests.get(url).json()
            
            if "result" in response and len(response["result"]) > 0:
                for update in response["result"]:
                    last_update_id = update["update_id"]
                    if "message" in update and "text" in update["message"]:
                        my_chat_id = update["message"]["chat"]["id"] 
                        user_text = update["message"]["text"]
                        
                        # 👑 COMMANDS LOGIC
                        if user_text == "/start":
                            send_msg(my_chat_id, "👑 V5.0 ULTIMATE TERMINAL ON!\nSystem Live & Reading Markets.")
                        
                        elif user_text in ["⏸ Pause Bot", "/pause"]:
                            bot_paused = True
                            send_msg(my_chat_id, "🛑 Bot PAUSED.")
                        elif user_text in ["▶️ Resume Bot", "/resume"]:
                            bot_paused = False
                            send_msg(my_chat_id, "✅ Bot RESUMED.")
                            
                        elif user_text == "🔕 Mute Alerts":
                            alerts_muted = True
                            send_msg(my_chat_id, "🔕 Alerts Muted. (Trades will run silently)")
                        elif user_text == "🔔 Unmute Alerts":
                            alerts_muted = False
                            send_msg(my_chat_id, "🔔 Alerts ON.")

                        elif user_text == "📅 Today Report":
                            today = get_ist_time().strftime("%Y-%m-%d")
                            conn = sqlite3.connect('trades.db')
                            c = conn.cursor()
                            c.execute("SELECT SUM(pnl) FROM pro_trades WHERE date LIKE ? AND status!='OPEN'", (f"{today}%",))
                            pnl = c.fetchone()[0] or 0
                            c.execute("SELECT COUNT(*) FROM pro_trades WHERE date LIKE ? AND status='PROFIT ✅'", (f"{today}%",))
                            wins = c.fetchone()[0]
                            c.execute("SELECT COUNT(*) FROM pro_trades WHERE date LIKE ? AND status!='OPEN'", (f"{today}%",))
                            total = c.fetchone()[0]
                            conn.close()
                            send_msg(my_chat_id, f"📅 *Today's Live Report*\n\n💰 Total PnL: ₹{pnl:.2f}\n📊 Trades Taken: {total}\n✅ Winning Trades: {wins}")

                        elif user_text == "📉 Drawdown":
                            conn = sqlite3.connect('trades.db')
                            c = conn.cursor()
                            c.execute("SELECT pnl FROM pro_trades WHERE status!='OPEN' ORDER BY id ASC")
                            rows = c.fetchall()
                            conn.close()
                            cum_pnl, peak, max_dd = 0, 0, 0
                            for r in rows:
                                cum_pnl += r[0]
                                if cum_pnl > peak: peak = cum_pnl
                                dd = peak - cum_pnl
                                if dd > max_dd: max_dd = dd
                            send_msg(my_chat_id, f"📉 *Risk Analysis*\n\n🔥 Max Drawdown: ₹{max_dd:.2f}\n(Peak se maximum loss)")

                        elif user_text == "/safe":
                            strategy_mode = "SAFE"
                            send_msg(my_chat_id, "🛡️ Mode: *SAFE*\nBot will only take strong setups (RSI > 60).")
                        elif user_text == "/agg":
                            strategy_mode = "AGGRESSIVE"
                            send_msg(my_chat_id, "⚡ Mode: *AGGRESSIVE*\nBot will take early entries (RSI > 50).")

                        elif user_text in ["📊 Check Status", "/status"]:
                            st = "⏸ Paused" if bot_paused else "▶️ Active"
                            al = "🔕 Muted" if alerts_muted else "🔔 ON"
                            send_msg(my_chat_id, f"📡 *System Status:*\n\n🔄 Bot: {st}\n🧠 Mode: {strategy_mode}\n🛡️ Risk: {current_risk_percent}%\n🔔 Alerts: {al}")
                            
                        elif user_text in ["💰 View PnL", "/pnl"]:
                            conn = sqlite3.connect('trades.db')
                            c = conn.cursor()
                            c.execute("SELECT SUM(pnl) FROM pro_trades WHERE status!='OPEN'")
                            total = c.fetchone()[0] or 0
                            conn.close()
                            send_msg(my_chat_id, f"💰 *Net PnL:* ₹{total:.2f}")
                            
                        elif user_text in ["📊 Win Rate", "/winrate"]:
                            conn = sqlite3.connect('trades.db')
                            c = conn.cursor()
                            c.execute("SELECT COUNT(*) FROM pro_trades WHERE status='PROFIT ✅'")
                            wins = c.fetchone()[0]
                            c.execute("SELECT COUNT(*) FROM pro_trades WHERE status='LOSS ❌' OR status='TSL HIT ⚠️'")
                            losses = c.fetchone()[0]
                            total = wins + losses
                            rate = (wins/total*100) if total > 0 else 0
                            conn.close()
                            send_msg(my_chat_id, f"📊 *Performance Stats*\n\n🏆 Win Rate: {rate:.2f}%\n✅ Wins: {wins}\n❌ Losses: {losses}\n⚖️ Total Trades: {total}")
                            
                        elif user_text in ["📂 Open Trades", "/open"]:
                            conn = sqlite3.connect('trades.db')
                            c = conn.cursor()
                            c.execute("SELECT symbol, type, entry_price FROM pro_trades WHERE status='OPEN'")
                            rows = c.fetchall()
                            if rows:
                                msg = "⚡ *LIVE OPEN TRADES:*\n\n"
                                for r in rows: msg += f"🔹 {r[0]} | {r[1]} @ ₹{r[2]:.2f}\n"
                                send_msg(my_chat_id, msg)
                            else: send_msg(my_chat_id, "Abhi koi trade open nahi hai.")
                            conn.close()
                            
                        elif user_text in ["❌ Close All", "/closeall"]:
                            emergency_close_all()
                            
                        elif user_text == "⚙️ Set Risk":
                            send_msg(my_chat_id, "⚙️ Change Risk using commands:\n\n👉 `/risk 1` (For 1%)\n👉 `/risk 2.5` (For 2.5%)")
                            
                        elif user_text.startswith("/risk "):
                            try:
                                current_risk_percent = float(user_text.split(" ")[1])
                                send_msg(my_chat_id, f"✅ Done! Risk set to *{current_risk_percent}%* per trade.")
                            except: send_msg(my_chat_id, "⚠️ Type like this: `/risk 2`")
                            
                        elif user_text in ["📉 Market Trend", "/trend"]:
                            msg = "📉 *LIVE MARKET TREND (15M):*\n\n"
                            symbols = ['NIFTY', 'BANKNIFTY', 'CNXFINANCE']
                            for sym in symbols:
                                try:
                                    data = tv.get_hist(symbol=sym, exchange='NSE', interval=Interval.in_15_minute, n_bars=200)
                                    ema200 = data['close'].ewm(span=200, adjust=False).mean().iloc[-1]
                                    cp = data['close'].iloc[-1]
                                    trend = "Bullish 🟢" if cp > ema200 else "Bearish 🔴"
                                    msg += f"🔹 {sym}: {trend}\n"
                                except: pass
                            send_msg(my_chat_id, msg)
                            
                        elif user_text == "📜 Show History":
                            conn = sqlite3.connect('trades.db')
                            c = conn.cursor()
                            c.execute('SELECT date, symbol, type, pnl, status FROM pro_trades WHERE status!="OPEN" ORDER BY id DESC LIMIT 5')
                            rows = c.fetchall()
                            if rows:
                                hist = "📂 *Last 5 Executions:*\n\n"
                                for r in rows: hist += f"🗓 {r[0]}\n{r[1]} | {r[2]} | {r[4]} | ₹{r[3]:.2f}\n\n"
                                send_msg(my_chat_id, hist)
                            else: send_msg(my_chat_id, "No history found.")
                            conn.close()
                                
            time.sleep(1)
        except Exception as e:
            time.sleep(5)

if __name__ == "__main__":
    setup_db()
    Thread(target=run_server, daemon=True).start()
    Thread(target=market_scanner_thread, daemon=True).start()
    telegram_listener()
