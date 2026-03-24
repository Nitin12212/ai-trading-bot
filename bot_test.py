import requests
import time
import pandas as pd
import numpy as np
import sqlite3
import os
import logging
import random
import psycopg2 
import urllib.parse 
import xml.etree.ElementTree as ET 
from psycopg2 import pool
from datetime import datetime, timedelta, time as dt_time 
from zoneinfo import ZoneInfo 
from threading import Thread, Lock
from queue import Queue
from flask import Flask, render_template_string, jsonify, request
from waitress import serve 

import yfinance as yf
import xgboost as xgb
from sklearn.model_selection import train_test_split

try:
    from alice_blue import AliceBlue
except ImportError:
    AliceBlue = None

# ==========================================
# 🛡️ 1. SECURITY & GLOBALS
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not TOKEN: raise ValueError("❌ TELEGRAM_BOT_TOKEN missing in Environment Variables!")

BASE_URL = f"https://api.telegram.org/bot{TOKEN}"
DB_URL = os.getenv("DATABASE_URL")
WEB_SECRET = os.getenv("WEB_SECRET", "12345")
DASHBOARD_URL = os.getenv("DASHBOARD_URL", "https://ai-trading-bot-itc0.onrender.com")

try: AUTHORIZED_USER = int(os.getenv("AUTHORIZED_USER", "0"))
except: AUTHORIZED_USER = 0
if AUTHORIZED_USER == 0: logging.error("🚨 CRITICAL: AUTHORIZED_USER not set!")

scan_lock = Lock()
data_lock = Lock() 
pending_trades = {}

IST = ZoneInfo("Asia/Kolkata")
def get_ist(): return datetime.now(IST)

db_pool = None
if DB_URL:
    try:
        urllib.parse.uses_netloc.append("postgres")
        result = urllib.parse.urlparse(DB_URL)
        db_pool = pool.SimpleConnectionPool(1, 20,
            user=result.username,
            password=result.password,
            host=result.hostname,
            port=result.port or 5432,
            database=result.path[1:],
            sslmode='require'
        )
        logging.info("✅ PostgreSQL Connection Pool Initialized.")
    except Exception as e: logging.error(f"❌ DB pool error: {e}")

# ==========================================
# 🚀 2. TELEGRAM MESSAGE QUEUE 
# ==========================================
msg_queue = Queue()

def _send_msg_raw(chat_id, text, reply_markup=None):
    if not TOKEN or not chat_id: return
    url = f"{BASE_URL}/sendMessage"
    
    default_keyboard = {
        "keyboard": [
            [{"text": "📊 Check Status"}, {"text": "📈 Live PnL"}],
            [{"text": "⚙️ Backtest"}, {"text": "🔍 Scan Now"}],
            [{"text": "🛡️ Safe Mode"}, {"text": "⚡ Aggressive Mode"}],
            [{"text": "🔄 Switch Mode"}, {"text": "🌐 Open Dashboard"}], 
            [{"text": "❌ Close All"}, {"text": "🎛️ Active Markets"}],
            [{"text": "⏸ Pause Bot"}, {"text": "▶️ Resume Bot"}]
        ], "resize_keyboard": True
    }
    
    payload = {
        "chat_id": chat_id, 
        "text": text, 
        "parse_mode": "Markdown", 
        "disable_web_page_preview": True,
        "reply_markup": reply_markup if reply_markup else default_keyboard
    }
    
    for _ in range(3):
        try: 
            requests.post(url, json=payload, timeout=5)
            return
        except Exception: time.sleep(2)

def telegram_worker():
    while True:
        try: 
            chat_id, text, reply_markup = msg_queue.get()
            _send_msg_raw(chat_id, text, reply_markup)
            msg_queue.task_done()
            time.sleep(1.2)  
        except: time.sleep(2)

Thread(target=telegram_worker, daemon=True).start()

def send_msg(chat_id, text, reply_markup=None):
    msg_queue.put((chat_id, text, reply_markup))

# ==========================================
# 🌐 3. DASHBOARD API & COMMAND CENTER
# ==========================================
app = Flask(__name__)

@app.before_request
def auth():
    if request.path in ['/webhook', '/api/command']: return 
    key = request.args.get("key") or request.headers.get("x-api-key")
    if key != WEB_SECRET: return "Unauthorized Access. System Locked.", 401

HTML_TEMPLATE = """
<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>AI Quant Dashboard</title><script src="https://cdn.tailwindcss.com"></script><script src="https://cdn.jsdelivr.net/npm/chart.js"></script><style>body { background-color: #0f172a; color: #f8fafc; font-family: 'Inter', sans-serif; } .glass-card { background: rgba(30, 41, 59, 0.7); backdrop-filter: blur(10px); border: 1px solid rgba(255, 255, 255, 0.1); }</style></head><body class="p-4 sm:p-6"><div class="max-w-md mx-auto"><div class="flex justify-between items-center mb-6"><div><h1 class="text-2xl font-bold text-emerald-400">V28.2 God Mode</h1><p class="text-xs text-slate-400">Approve/Reject Mode Active</p></div><div id="status-badge" class="px-3 py-1 rounded-full text-xs font-bold bg-emerald-500/20 text-emerald-400 border border-emerald-500/50">● ACTIVE</div></div><div class="grid grid-cols-2 gap-4 mb-6"><div class="glass-card p-4 rounded-xl text-center"><p class="text-xs text-slate-400 mb-1">Total PnL</p><p id="total-pnl" class="text-xl font-bold text-white">₹0.00</p></div><div class="glass-card p-4 rounded-xl text-center"><p class="text-xs text-slate-400 mb-1">Win Rate</p><p id="win-rate" class="text-xl font-bold text-blue-400">0%</p></div><div class="glass-card p-4 rounded-xl text-center"><p class="text-xs text-slate-400 mb-1">Total Trades</p><p id="total-trades" class="text-xl font-bold text-white">0</p></div><div class="glass-card p-4 rounded-xl text-center"><p class="text-xs text-slate-400 mb-1">Dynamic Capital</p><p id="dynamic-cap" class="text-xl font-bold text-purple-400">₹50K</p></div></div><h2 class="text-lg font-bold text-slate-300 mb-3">📈 Equity Curve</h2><div class="glass-card p-4 rounded-xl mb-6"><canvas id="equityChart" height="200"></canvas></div><h2 class="text-lg font-bold text-slate-300 mb-3 mt-6">🎛️ Command Center</h2><div class="grid grid-cols-2 gap-2 mb-6"><button onclick="sendCommand('📊 Check Status')" class="bg-blue-600/20 text-blue-400 border border-blue-600/50 p-2 rounded text-sm font-bold hover:bg-blue-600/40">📊 Status</button><button onclick="sendCommand('📈 Live PnL')" class="bg-emerald-600/20 text-emerald-400 border border-emerald-600/50 p-2 rounded text-sm font-bold hover:bg-emerald-600/40">📈 Live PnL</button><button onclick="sendCommand('/backtest')" class="bg-purple-600/20 text-purple-400 border border-purple-600/50 p-2 rounded text-sm font-bold hover:bg-purple-600/40">⚙️ Backtest</button><button onclick="sendCommand('🎛️ Active Markets')" class="bg-indigo-600/20 text-indigo-400 border border-indigo-600/50 p-2 rounded text-sm font-bold hover:bg-indigo-600/40">🎛️ Markets</button><button onclick="sendCommand('🛡️ Safe Mode')" class="bg-sky-600/20 text-sky-400 border border-sky-600/50 p-2 rounded text-sm font-bold hover:bg-sky-600/40">🛡️ Safe</button><button onclick="sendCommand('⚡ Aggressive Mode')" class="bg-orange-600/20 text-orange-400 border border-orange-600/50 p-2 rounded text-sm font-bold hover:bg-orange-600/40">⚡ Aggr</button><button onclick="sendCommand('⏸ Pause Bot')" class="bg-amber-600/20 text-amber-400 border border-amber-600/50 p-2 rounded text-sm font-bold hover:bg-amber-600/40">⏸ Pause</button><button onclick="sendCommand('▶️ Resume Bot')" class="bg-emerald-600/20 text-emerald-400 border border-emerald-600/50 p-2 rounded text-sm font-bold hover:bg-emerald-600/40">▶️ Resume</button><button onclick="sendCommand('❌ Close All')" class="bg-rose-600/20 text-rose-400 border border-rose-600/50 p-2 rounded text-sm font-bold hover:bg-rose-600/40 col-span-2">❌ Close All Positions</button></div><h2 class="text-lg font-bold text-slate-300 mb-3">⚡ Live Open Trades</h2><div id="open-trades-container" class="space-y-3"><div class="text-center text-slate-500 text-sm py-4">Loading trades...</div></div></div><script>const urlParams = new URLSearchParams(window.location.search); const authKey = urlParams.get('key') || ''; let equityChartInstance = null; async function fetchEquityData() { try { const res = await fetch('/api/equity?key=' + authKey); const rawData = await res.json(); let labels = ["Start"]; let capital = 50000; let dataPoints = [capital]; rawData.forEach(trade => { capital += trade.pnl; labels.push(trade.date.split(" ")[0]); dataPoints.push(capital); }); const ctx = document.getElementById('equityChart').getContext('2d'); if(equityChartInstance) { equityChartInstance.data.labels = labels; equityChartInstance.data.datasets[0].data = dataPoints; equityChartInstance.update(); } else { equityChartInstance = new Chart(ctx, { type: 'line', data: { labels: labels, datasets: [{ label: 'Capital (₹)', data: dataPoints, borderColor: '#34d399', backgroundColor: 'rgba(52, 211, 153, 0.1)', borderWidth: 2, fill: true, tension: 0.4, pointRadius: 1, pointBackgroundColor: '#fff' }] }, options: { responsive: true, plugins: { legend: { display: false } }, scales: { x: { display: false }, y: { ticks: { color: '#94a3b8' }, grid: { color: 'rgba(255, 255, 255, 0.05)' } } } } }); } } catch(e) {} } async function fetchStats() { try { const res = await fetch('/api/stats?key=' + authKey); const data = await res.json(); document.getElementById('total-pnl').innerText = '₹' + data.pnl.toFixed(2); document.getElementById('total-pnl').className = data.pnl >= 0 ? 'text-xl font-bold text-emerald-400' : 'text-xl font-bold text-rose-400'; document.getElementById('win-rate').innerText = data.win_rate.toFixed(1) + '%'; document.getElementById('total-trades').innerText = data.total_trades; document.getElementById('dynamic-cap').innerText = '₹' + (50000 + data.pnl).toLocaleString(); const badge = document.getElementById('status-badge'); if (data.paused) { badge.innerText = '⏸ PAUSED'; badge.className = 'px-3 py-1 rounded-full text-xs font-bold bg-amber-500/20 text-amber-400 border border-amber-500/50'; } else { badge.innerText = '● ACTIVE'; badge.className = 'px-3 py-1 rounded-full text-xs font-bold bg-emerald-500/20 text-emerald-400 border border-emerald-500/50'; } const tradesContainer = document.getElementById('open-trades-container'); if (data.open_trades.length === 0) { tradesContainer.innerHTML = '<div class="glass-card p-4 rounded-xl text-center text-slate-500 text-sm">No open trades right now.</div>'; } else { let html = ''; data.open_trades.forEach(t => { const typeColor = t[1].includes('BUY') ? 'text-emerald-400' : 'text-rose-400'; const partialTag = t[5] ? '<span class="ml-2 text-[10px] bg-blue-500/20 text-blue-400 px-1 rounded">50% BOOKED</span>' : ''; html += `<div class="glass-card p-4 rounded-xl flex justify-between items-center"><div><p class="font-bold text-white text-sm">${t[0]} ${partialTag}</p><p class="text-[10px] text-slate-400">Entry: ₹${t[2].toFixed(2)}</p></div><div class="text-right"><p class="font-bold text-sm ${typeColor}">${t[1]}</p><p class="text-[10px] text-slate-400">Qty: ${t[6]}</p></div></div>`; }); tradesContainer.innerHTML = html; } } catch (e) {} } async function sendCommand(cmd) { try { const res = await fetch('/api/command', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({command: cmd, secret: authKey})}); const data = await res.json(); fetchStats(); } catch(e) {} } fetchStats(); fetchEquityData(); setInterval(fetchStats, 5000); setInterval(fetchEquityData, 10000); </script></body></html>
"""

@app.route('/')
def dashboard(): return render_template_string(HTML_TEMPLATE)

@app.route('/api/stats')
def api_stats():
    pnl = get_val("SELECT SUM(pnl) FROM pro_trades WHERE status!='OPEN'")
    total = int(get_val("SELECT COUNT(*) FROM pro_trades WHERE status!='OPEN'"))
    wins = int(get_val("SELECT COUNT(*) FROM pro_trades WHERE status='PROFIT ✅'"))
    open_trades = execute_db("SELECT symbol, type, entry_price, mode, sl, partial_exit, qty FROM pro_trades WHERE status='OPEN'", fetchall=True) or []
    return jsonify({"pnl": pnl, "win_rate": (wins/total*100) if total>0 else 0, "total_trades": total, "open_trades": open_trades, "mode": trading_mode, "paused": bot_paused})

@app.route('/api/equity')
def api_equity():
    data = execute_db("SELECT date, pnl FROM pro_trades WHERE status!='OPEN' ORDER BY date_ts ASC", fetchall=True) or []
    if not data: return jsonify([])
    capital, curve = 50000, []
    for r in data:
        capital += r[1]
        curve.append(capital)
    smoothed = pd.Series(curve).rolling(5, min_periods=1).mean().tolist()
    response = [{"date": data[i][0], "pnl": smoothed[i] - (smoothed[i-1] if i > 0 else 50000)} for i in range(len(smoothed))]
    return jsonify(response)

@app.route('/api/command', methods=['POST'])
def api_command():
    try:
        data = request.json
        if data.get('secret') != WEB_SECRET: return jsonify({"error": "Unauthorized"}), 401
        cmd = data.get('command')
        Thread(target=process_command, args=(AUTHORIZED_USER, cmd)).start()
        return jsonify({"status": f"Command '{cmd}' Executed."}), 200
    except Exception as e: return jsonify({"error": str(e)}), 500

def run_server():
    port = int(os.environ.get("PORT", 8080))
    serve(app, host='0.0.0.0', port=port)

# ==========================================
# 🗄️ 4. DATABASE POOL
# ==========================================
def execute_db(query, params=(), fetch=False, fetchall=False):
    if not db_pool: return None
    conn = db_pool.getconn()
    try:
        with conn.cursor() as c:
            c.execute(query, params)
            if fetch: res = c.fetchone()
            elif fetchall: res = c.fetchall()
            else: conn.commit(); res = True
        return res
    except Exception as e:
        conn.rollback(); return None
    finally:
        db_pool.putconn(conn)

def get_val(query, params=()):
    res = execute_db(query, params, fetch=True)
    return float(res[0]) if res and res[0] is not None else 0.0

def setup_db():
    execute_db('''CREATE TABLE IF NOT EXISTS pro_trades 
                 (id SERIAL PRIMARY KEY, date TEXT, date_ts INTEGER, symbol TEXT, type TEXT, 
                  entry_price REAL, sl REAL, tp REAL, status TEXT, pnl REAL, mode TEXT, 
                  partial_exit INTEGER DEFAULT 0, qty INTEGER DEFAULT 0, features TEXT DEFAULT '')''')

last_signal = {}
def recover_state():
    rows = execute_db("SELECT symbol FROM pro_trades WHERE status='OPEN'", fetchall=True)
    if rows:
        with data_lock:
            for r in rows: last_signal[r[0]] = "RECOVERED"

# ==========================================
# 🤖 5. XGBOOST ML ENGINE
# ==========================================
ml_model = None
last_train_time = 0

def get_ml_prediction(rsi, macd, dist, pcr, vix, smc_score):
    global ml_model, last_train_time
    try:
        if time.time() - last_train_time > 180 or ml_model is None:
            rows = execute_db("SELECT features, status FROM pro_trades WHERE status!='OPEN' AND features!=''", fetchall=True)
            if rows and len(rows) >= 50:
                X, y = [], []
                for feat_str, status in rows:
                    try:
                        parts = feat_str.split(',')
                        X.append([
                            float(parts[0].split(':')[1]), float(parts[1].split(':')[1]), float(parts[2].split(':')[1]),
                            float(parts[3].split(':')[1]) if len(parts) > 3 else 1.0,
                            float(parts[4].split(':')[1]) if len(parts) > 4 else 1.0,
                            float(parts[5].split(':')[1]) if len(parts) > 5 else 0
                        ])
                        y.append(1 if "PROFIT" in status else 0)
                    except: continue
                if len(X) >= 50 and y.count(1) >= 5 and y.count(0) >= 5:
                    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
                    clf = xgb.XGBClassifier(n_estimators=100, max_depth=4, learning_rate=0.1, random_state=42, eval_metric='logloss')
                    clf.fit(X_train, y_train)
                    ml_model = clf
                    last_train_time = time.time()
        
        if ml_model:
            return ml_model.predict_proba(np.array([[rsi, macd, dist, pcr, vix, smc_score]]))[0][1] * 100
        return None
    except: return None

last_vix, last_vix_time = 1.0, 0
def get_vix_multiplier():
    global last_vix, last_vix_time
    if time.time() - last_vix_time < 300: return last_vix
    try:
        vix_data = yf.download('^INDIAVIX', period='5d', interval='1d', progress=False)
        if vix_data is not None and not vix_data.empty:
            v = vix_data['Close'].iloc[:, 0] if isinstance(vix_data.columns, pd.MultiIndex) else vix_data['Close']
            vix = v.iloc[-1]
            last_vix_time = time.time()
            if vix > 22: last_vix = 0.5   
            elif vix < 13: last_vix = 1.5 
            else: last_vix = 1.0
        return last_vix                
    except: return last_vix

def get_pcr(symbol): return 1.0 

def check_smc(data):
    try:
        if len(data) < 10: return "NEUTRAL" 
        recent_low = data['low'].rolling(10).min().iloc[-2]
        if data['low'].iloc[-1] <= recent_low and data['close'].iloc[-1] > recent_low: return "SMC_BULLISH"
        return "NEUTRAL"
    except: return "NEUTRAL"

def is_news_time():
    now = get_ist()
    if now.minute in [29, 59]: return True 
    return False 

def get_sentiment(sym): return 0

# ==========================================
# 🏦 6. REAL BROKER MOCK
# ==========================================
def place_real_order(symbol, decision_full_text, exec_price, qty):
    logging.info(f"REAL ORDER MOCK: {decision_full_text} x {qty}")
    return True

# ==========================================
# 🧠 7. CORE LOGIC
# ==========================================
bot_paused = False
trading_mode = "DEMO"
strategy_mode = "SAFE"
alerts_muted = False
current_risk_percent = 2.0
daily_loss_limit, daily_profit_target, global_drawdown_limit = -2000.0, 3000.0, -5000.0
max_daily_trades = 5          
trade_cooldown_seconds = 300  
last_trade_time = {}          
active_symbols = ['NIFTY', 'BANKNIFTY', 'CNXFINANCE', 'SENSEX', 'BANKEX'] 
options_lot_size = {"NIFTY": 50, "BANKNIFTY": 15, "CNXFINANCE": 40, "SENSEX": 10, "BANKEX": 15} 
yf_symbol_map = {"NIFTY": "^NSEI", "BANKNIFTY": "^NSEBANK", "CNXFINANCE": "NIFTY_FIN_SERVICE.NS", "SENSEX": "^BSESN", "BANKEX": "BSE-BANK.BO"}

def get_yf_col(df, col):
    return df[col].iloc[:, 0] if isinstance(df.columns, pd.MultiIndex) else df[col]

def process_single_symbol(sym, manual=False):
    global strategy_mode, alerts_muted, current_risk_percent, bot_paused, trading_mode, pending_trades
    
    if bot_paused: return f"⏸ {sym}: Paused" if manual else None
    if is_news_time(): return f"📰 {sym}: News Time Block" if manual else None
    
    with data_lock:
        if sym in last_trade_time and time.time() - last_trade_time[sym] < trade_cooldown_seconds: return f"⏳ {sym}: Cooldown" if manual else None
    
    yf_sym = yf_symbol_map.get(sym, sym)
    try:
        d1 = yf.download(yf_sym, period='2d', interval='1m', progress=False)
        d5 = yf.download(yf_sym, period='5d', interval='5m', progress=False)
        d15 = yf.download(yf_sym, period='5d', interval='15m', progress=False)
        if d1.empty or d5.empty or d15.empty: return f"❌ {sym}: YF Data Empty" if manual else None
        
        data_1m = pd.DataFrame({'close': get_yf_col(d1, 'Close')})
        data_5m = pd.DataFrame({'close': get_yf_col(d5, 'Close'), 'high': get_yf_col(d5, 'High'), 'low': get_yf_col(d5, 'Low'), 'volume': get_yf_col(d5, 'Volume')})
        data_15m = pd.DataFrame({'close': get_yf_col(d15, 'Close')})
    except Exception as e: return f"❌ {sym}: YF API Error ({str(e)[:15]})" if manual else None

    if len(data_5m) < 20 or len(data_15m) < 20: return f"❌ {sym}: Not enough data" if manual else None
    
    cp = data_5m['close'].iloc[-1]
    
    if 'volume' in data_5m.columns and data_5m['volume'].iloc[-1] > 0:
        vol_avg = data_5m['volume'].rolling(20).mean().iloc[-1]
        if pd.notna(vol_avg) and data_5m['volume'].iloc[-1] < vol_avg: return f"📉 {sym}: Low Volume" if manual else None

    atr = (data_5m['high'] - data_5m['low']).rolling(14).mean().iloc[-1]
    if pd.isna(atr) or (atr / cp) < 0.0002: return f"📉 {sym}: Low Volatility" if manual else None
    
    ema200 = data_5m['close'].ewm(span=200, adjust=False).mean().iloc[-1]
    trend_15m_up = data_15m['close'].iloc[-1] > data_15m['close'].ewm(span=50).mean().iloc[-1]
    
    delta = data_5m['close'].diff()
    gain, loss = delta.clip(lower=0).ewm(alpha=1/14, adjust=False).mean(), -delta.clip(upper=0).ewm(alpha=1/14, adjust=False).mean()
    if loss.iloc[-1] == 0: return f"➖ {sym}: Flat (RSI)" if manual else None
    rsi = 100 - (100 / (1 + (gain / loss))).iloc[-1]
    
    ema12, ema26 = data_5m['close'].ewm(span=12).mean(), data_5m['close'].ewm(span=26).mean()
    macd, macd_sig = ema12 - ema26, (ema12 - ema26).ewm(span=9).mean()
    
    open_trades = execute_db("SELECT id, type, entry_price, sl, tp, pnl, partial_exit, qty, date_ts FROM pro_trades WHERE symbol=%s AND status='OPEN'", (sym,), fetchall=True)
    if open_trades:
        for t in open_trades:
            t_id, t_type, entry, sl, tp, current_pnl, partial_exit, qty, date_ts = t
            status, pnl, msg = "OPEN", current_pnl, None
            pts_captured = (cp - entry) if "BUY" in t_type else (entry - cp)
            half_target = entry + (tp - entry)/2 if "BUY" in t_type else entry - (entry - tp)/2
            
            if int(time.time()) - date_ts > 1800:
                status, pnl, msg = "CLOSED ⏱️ (Time Decay)", pts_captured * qty, f"⏱️ *TIME EXIT (30m):* {sym} (PnL: ₹{pts_captured * qty:.2f})"
            elif not partial_exit and ((cp >= half_target if "BUY" in t_type else cp <= half_target)):
                execute_db("UPDATE pro_trades SET partial_exit=1, pnl=%s, sl=%s WHERE id=%s", (pts_captured * (qty//2), entry, t_id))
                sl = entry 
                if not alerts_muted: send_msg(AUTHORIZED_USER, f"🎯 *PARTIAL BOOKED (50%)*: {sym}\n🛡️ SL moved to Entry.")
                continue 
            elif pts_captured > atr * 2.5:
                new_sl = entry + (pts_captured * 0.65) if "BUY" in t_type else entry - (pts_captured * 0.65)
                if ("BUY" in t_type and new_sl > sl) or ("SELL" in t_type and new_sl < sl): execute_db("UPDATE pro_trades SET sl=%s WHERE id=%s", (new_sl, t_id)); sl = new_sl
            else:
                if t_type == "BUY 🟢" and cp > entry:
                    new_sl = max(sl, cp - (cp * 0.001))
                    if new_sl > sl: execute_db("UPDATE pro_trades SET sl=%s WHERE id=%s", (new_sl, t_id)); sl = new_sl
                elif t_type == "SELL 🔴" and cp < entry:
                    new_sl = min(sl, cp + (cp * 0.001))
                    if new_sl < sl: execute_db("UPDATE pro_trades SET sl=%s WHERE id=%s", (new_sl, t_id)); sl = new_sl

            rem_qty = (qty // 2) if partial_exit else qty
            if status == "OPEN":
                if t_type == "BUY 🟢":
                    if cp >= tp: status, pnl, msg = "PROFIT ✅", current_pnl + (abs(tp - entry) * rem_qty), f"🎯 TARGET HIT: {sym}"
                    elif cp <= sl: status, pnl, msg = "LOSS ❌", current_pnl - (abs(entry - cp) * rem_qty), f"🛑 SL HIT: {sym}"
                elif t_type == "SELL 🔴":
                    if cp <= tp: status, pnl, msg = "PROFIT ✅", current_pnl + (abs(entry - tp) * rem_qty), f"🎯 TARGET HIT: {sym}"
                    elif cp >= sl: status, pnl, msg = "LOSS ❌", current_pnl - (abs(sl - entry) * rem_qty), f"🛑 SL HIT: {sym}"
            
            if status != "OPEN":
                execute_db("UPDATE pro_trades SET status=%s, pnl=%s WHERE id=%s", (status, pnl, t_id))
                if not alerts_muted: send_msg(AUTHORIZED_USER, msg)
        return f"🔄 {sym}: Trade Managed" if manual else None
    
    dist_ema = (abs(cp - ema200) / ema200) * 100
    if dist_ema > (1.5 if strategy_mode == "SAFE" else 3.0): return f"📈 {sym}: Far from EMA" if manual else None

    if ((data_5m['high'].iloc[-1] - data_5m['low'].iloc[-1]) / cp) > 0.003: return f"🛑 {sym}: High Spread" if manual else None
    if abs(macd.iloc[-1] - macd_sig.iloc[-1]) < 0.01: return f"➖ {sym}: Weak MACD" if manual else None

    vix_multi, pcr, smc_signal = get_vix_multiplier(), get_pcr(sym), check_smc(data_5m)
    
    decision, confidence = "WAIT", 0
    if cp > ema200:
        if rsi > (60 if strategy_mode == "SAFE" else 50): confidence += 1
        if macd.iloc[-1] > macd_sig.iloc[-1]: confidence += 1
        if trend_15m_up: confidence += 1
        if pcr >= 0.8: confidence += 1
        if smc_signal == "SMC_BULLISH": confidence += 1
        if confidence >= 2: decision = "BUY 🟢" 
    elif cp < ema200:
        if rsi < (40 if strategy_mode == "SAFE" else 50): confidence += 1
        if macd.iloc[-1] < macd_sig.iloc[-1]: confidence += 1
        if not trend_15m_up: confidence += 1
        if pcr <= 1.2: confidence += 1
        if smc_signal == "SMC_BEARISH": confidence += 1
        if confidence >= 2: decision = "SELL 🔴"

    if decision == "WAIT": return f"⏳ {sym}: No Setup (Conf: {confidence}/5)" if manual else None
    
    prev_c = data_5m['close'].iloc[-2]
    if (decision == "BUY 🟢" and cp <= prev_c) or (decision == "SELL 🔴" and cp >= prev_c): return f"⏳ {sym}: Waiting Breakout" if manual else None
    
    sent = get_sentiment(sym)
    if (sent == -1 and decision == "BUY 🟢") or (sent == 1 and decision == "SELL 🔴"): return f"📰 {sym}: Sentiment Blocked" if manual else None

    with data_lock:
        if sym in last_signal and last_signal[sym] == decision: return None
        last_signal[sym] = decision

    smc_score = 1 if smc_signal == "SMC_BULLISH" else (-1 if smc_signal == "SMC_BEARISH" else 0)
    ml_prob = get_ml_prediction(rsi, macd.iloc[-1], dist_ema, pcr, vix_multi, smc_score)
    if ml_prob is not None and ml_prob < 50.0: return f"🤖 {sym}: ML AI Rejected" if manual else None
    
    slippage = max(((data_5m['high'].iloc[-1] - data_5m['low'].iloc[-1]) * 0.1), cp * 0.0002) 
    if slippage > (cp * 0.002): return f"🛑 {sym}: Slippage Limit Hit" if manual else None
    
    exec_price = cp + slippage if "BUY" in decision else cp - slippage
    sl_dist = atr * 1.5
    sl = exec_price - sl_dist if "BUY" in decision else exec_price + sl_dist
    tp = exec_price + (atr * 4.0) if "BUY" in decision else exec_price - (atr * 4.0)
    
    if sl_dist <= 0: return f"❌ {sym}: Math SL Error" if manual else None

    rr = abs(tp - exec_price) / sl_dist
    win_prob = (ml_prob / 100.0) if ml_prob else 0.55
    kelly = (win_prob * rr - (1 - win_prob)) / rr if rr > 0 else 0
    if kelly <= 0: return f"📉 {sym}: Kelly Blocked" if manual else None

    adj_risk = min((1.0 if get_val("SELECT SUM(pnl) FROM pro_trades WHERE date LIKE %s AND status!='OPEN'", (f"{get_ist().strftime('%Y-%m-%d')}%",)) < 0 else current_risk_percent) * vix_multi, max(0.5, kelly * 100 * 0.5))
    dyn_cap = max(50000, 50000 + get_val("SELECT SUM(pnl) FROM pro_trades WHERE status!='OPEN'"))
    
    lot_size = options_lot_size.get(sym, 1) 
    qty = max(lot_size, int((dyn_cap * (adj_risk / 100)) / sl_dist // lot_size) * lot_size) 
    
    strike_step = 100 if sym in ["BANKNIFTY", "SENSEX", "BANKEX"] else 50
    atm_strike = int(round(cp / strike_step) * strike_step)
    opt_type = f"{atm_strike} CE" if "BUY" in decision else f"{atm_strike} PE"
    hedge_type = f"{atm_strike - (strike_step*2)} PE" if "BUY" in decision else f"{atm_strike + (strike_step*2)} CE"
        
    final_decision = f"{decision} | {opt_type} (Hedge: {hedge_type})"
    trade_id = f"{sym}_{int(time.time())}"
    
    pending_trades[trade_id] = {"sym": sym, "final_decision": final_decision, "exec_price": exec_price, "sl": sl, "tp": tp, "qty": qty, "features_str": f"RSI:{rsi:.1f},MACD:{macd.iloc[-1]:.2f},DIST:{dist_ema:.1f},PCR:{pcr:.2f},VIX:{vix_multi:.2f},SMC:{smc_score}"}
    
    inline_kb = {"inline_keyboard": [[{"text": "✅ Haan, Trade Le Lo", "callback_data": f"YES_{trade_id}"}], [{"text": "❌ Nahi, Reject Karo", "callback_data": f"NO_{trade_id}"}]]}
    send_msg(AUTHORIZED_USER, f"🚀 *TRADE SETUP FOUND* 🚀\n\n📈 *Symbol:* {sym}\n🤖 *Action:* {final_decision}\n🛒 *Qty:* {qty} (Risk {adj_risk:.1f}%)\n🧠 *XGB Edge:* {ml_prob if ml_prob else 0:.1f}%\n\n🔸 *Entry:* ₹{exec_price:.2f}\n🎯 *TP:* ₹{tp:.2f} | 🛡️ *SL:* ₹{sl:.2f}\n⚖️ *RR:* 1:{rr:.1f}\n\n👉 Trade Lena Hai?", inline_kb)

    return f"✅ {sym}: Waiting for Approval." if manual else None

def run_scan_cycle(manual=False):
    global bot_paused, current_risk_percent
    now = get_ist()
    if not manual and not (dt_time(9, 15) <= now.time() <= dt_time(15, 30)): return "SKIP" 
            
    total_pnl = get_val("SELECT SUM(pnl) FROM pro_trades WHERE status!='OPEN'")
    today_pnl = get_val("SELECT SUM(pnl) FROM pro_trades WHERE date LIKE %s AND status!='OPEN'", (f"{now.strftime('%Y-%m-%d')}%",))
    trades_today = int(get_val("SELECT COUNT(*) FROM pro_trades WHERE date LIKE %s", (f"{now.strftime('%Y-%m-%d')}%",)))
    
    if trades_today >= 3 and today_pnl < 0: return "PAUSE"
    last_5 = execute_db("SELECT pnl FROM pro_trades WHERE status!='OPEN' ORDER BY id DESC LIMIT 5", fetchall=True)
    if last_5 and sum([x[0] for x in last_5]) < -1000: return "PAUSE"
    if total_pnl <= global_drawdown_limit or trades_today >= max_daily_trades or today_pnl <= daily_loss_limit or today_pnl >= daily_profit_target: return "PAUSE"

    # 🔥 FIX 1: Removed ThreadPoolExecutor to prevent Yahoo Finance IP block. Scanning Sequentially.
    results = []
    for s in active_symbols:
        results.append(process_single_symbol(s, manual))
        time.sleep(1) # Small gap between requests so YF doesn't block
        
    if manual:
        valid_results = [r for r in results if r]
        send_msg(AUTHORIZED_USER, "🔍 *Diagnostic Scan Report:*\n\n" + "\n".join(valid_results) if valid_results else "⚠️ API Data Issue")
    return "CONTINUE"

def auto_scanner():
    global bot_paused
    while True:
        try:
            if not AUTHORIZED_USER: time.sleep(5); continue
            now = get_ist()
            if now.hour == 9 and now.minute == 14:
                with data_lock: last_signal.clear(); last_trade_time.clear()
                if bot_paused: bot_paused = False; send_msg(AUTHORIZED_USER, "🌅 *Good Morning!* Bot Auto-Resumed.")
            
            if bot_paused: time.sleep(5); continue
            if scan_lock.acquire(blocking=False):
                try:
                    if run_scan_cycle(False) == "PAUSE": bot_paused = True; send_msg(AUTHORIZED_USER, "🛑 Bot auto-paused.")
                finally: scan_lock.release()
        except: pass
        time.sleep(60)

# ==========================================
# 🎮 8. COMMAND HANDLER
# ==========================================
def process_command(chat_id, txt, cb_query_id=None):
    global bot_paused, trading_mode, strategy_mode, alerts_muted, max_daily_trades, active_symbols, pending_trades
    
    if cb_query_id:
        if txt.startswith("YES_"):
            trade_id = txt.replace("YES_", "")
            if trade_id in pending_trades:
                t = pending_trades.pop(trade_id)
                if trading_mode == "REAL": place_real_order(t["sym"], t["final_decision"], t["exec_price"], t["qty"])
                execute_db('INSERT INTO pro_trades (date, date_ts, symbol, type, entry_price, sl, tp, status, pnl, mode, qty, features) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                           (get_ist().strftime("%Y-%m-%d %H:%M"), int(time.time()), t["sym"], t["final_decision"], t["exec_price"], t["sl"], t["tp"], "OPEN", 0.0, trading_mode, t["qty"], t["features_str"]))
                with data_lock: last_trade_time[t["sym"]] = time.time()
                send_msg(chat_id, f"✅ *APPROVED!* \nSymbol: {t['sym']} entry triggered.")
            else: send_msg(chat_id, "❌ Trade expired.")
        elif txt.startswith("NO_"):
            trade_id = txt.replace("NO_", "")
            if trade_id in pending_trades:
                t = pending_trades.pop(trade_id)
                send_msg(chat_id, f"🚫 *REJECTED* \nSymbol: {t['sym']} cancelled.")
            else: send_msg(chat_id, "❌ Trade expired.")
        try: requests.post(f"{BASE_URL}/answerCallbackQuery", json={"callback_query_id": cb_query_id})
        except: pass
        return

    if txt == "/start": send_msg(chat_id, "👋 Hello boss I am ready! V28.2 The Ultimate God Mode Engine Online.")
    elif txt == "🎛️ Active Markets": send_msg(chat_id, f"🎛️ *Active Markets:* {', '.join(active_symbols)}")
    elif txt in ["/backtest", "⚙️ Backtest"]:
        rows = execute_db("SELECT pnl FROM pro_trades WHERE status!='OPEN' ORDER BY date_ts ASC", fetchall=True)
        if not rows: return send_msg(chat_id, "❌ Not enough data.")
        eq, max_dd, peak, wins = 50000, 0, 50000, 0
        for r in rows:
            eq += r[0]
            if eq > peak: peak = eq
            if (peak - eq) > max_dd: max_dd = peak - eq
            if r[0] > 0: wins += 1
        send_msg(chat_id, f"🔥 *180-Day DB Backtest:*\nTrades: {len(rows)}\nWin Rate: {(wins/len(rows)*100):.1f}%\nMax DD: ₹{max_dd:.0f}\nFinal Eq: ₹{eq:.0f}")
    elif txt.startswith("/add "):
        new_sym = txt.split(" ")[1].upper()
        if new_sym not in active_symbols: active_symbols.append(new_sym)
        send_msg(chat_id, f"✅ Added {new_sym}.")
    elif txt.startswith("/remove "):
        rem_sym = txt.split(" ")[1].upper()
        if rem_sym in active_symbols: active_symbols.remove(rem_sym)
        send_msg(chat_id, f"❌ Removed {rem_sym}.")
    elif txt == "🌐 Open Dashboard": requests.post(f"{BASE_URL}/sendMessage", json={"chat_id": chat_id, "text": "Access:", "reply_markup": {"inline_keyboard": [[{"text": "🚀 Dashboard", "url": f"{DASHBOARD_URL}?key={WEB_SECRET}"}]]}})
    elif txt in ["🔄 Switch Mode", "/mode"]: send_msg(chat_id, "⚠️ Type `CONFIRM REAL` or `CONFIRM DEMO`")
    elif txt == "CONFIRM REAL": trading_mode = "REAL"; send_msg(chat_id, "💰 *REAL ENABLED!*")
    elif txt == "CONFIRM DEMO": trading_mode = "DEMO"; send_msg(chat_id, "🛡️ *DEMO Mode.*")
    elif txt == "📊 Check Status": send_msg(chat_id, f"📡 Bot: {'Paused ⏸' if bot_paused else 'Active ▶️'}\n🧠 Mode: {trading_mode}\n🛡️ Strategy: {strategy_mode}\n📊 Max Trades: {max_daily_trades}/day")
    elif txt in ["💰 View PnL", "/pnl"]: send_msg(chat_id, f"💰 Net PnL: ₹{get_val('SELECT SUM(pnl) FROM pro_trades WHERE status!=\\'OPEN\\''):.2f}")
    elif txt == "📈 Live PnL":
        rows = execute_db("SELECT symbol, type, entry_price, qty FROM pro_trades WHERE status='OPEN'", fetchall=True)
        if not rows: send_msg(chat_id, "No open trades.")
        else:
            msg, tl = "📈 *LIVE OPEN TRADES:*\n\n", 0
            for sym, t_type, entry, qty in rows:
                try:
                    cp = yf.download(yf_symbol_map.get(sym, sym), period='1d', interval='1m', progress=False)['Close'].iloc[-1]
                    pnl = ((cp - entry) if "BUY" in t_type else (entry - cp)) * qty
                    tl += pnl; msg += f"🔹 {sym}: ₹{pnl:.2f}\n"
                except: pass
            send_msg(chat_id, msg + f"\n💰 *Total:* ₹{tl:.2f}")
    elif txt == "🔍 Scan Now": Thread(target=lambda: (scan_lock.acquire(blocking=False) and [run_scan_cycle(True), scan_lock.release()])).start()
    elif txt == "🛡️ Safe Mode": strategy_mode = "SAFE"; send_msg(chat_id, "🛡️ Safe Mode ON.")
    elif txt == "⚡ Aggressive Mode": strategy_mode = "AGGRESSIVE"; send_msg(chat_id, "⚡ Aggr Mode ON.")
    elif txt in ["❌ Close All", "/closeall"]:
        if execute_db("SELECT id FROM pro_trades WHERE status='OPEN'", fetchall=True): execute_db("UPDATE pro_trades SET status='CLOSED ⚠️' WHERE status='OPEN'"); send_msg(chat_id, "⚠️ All CLOSED.")
    elif txt in ["⏸ Pause Bot", "/pause"]: bot_paused = True; send_msg(chat_id, "🛑 Bot Paused.")
    elif txt in ["▶️ Resume Bot", "/resume"]: bot_paused = False; send_msg(chat_id, "✅ Bot Resumed.")

def telegram():
    last_id = None
    setup_db()
    recover_state()
    while True:
        try:
            url = f"{BASE_URL}/getUpdates?timeout=5"
            if last_id: url += f"&offset={last_id + 1}"
            res = requests.get(url, timeout=10).json()
            if "result" in res:
                for upd in res["result"]:
                    last_id = upd["update_id"]
                    if "callback_query" in upd:
                        cb = upd["callback_query"]
                        if cb["message"]["chat"]["id"] == AUTHORIZED_USER:
                            process_command(AUTHORIZED_USER, cb["data"], cb["id"])
                            requests.post(f"{BASE_URL}/editMessageReplyMarkup", json={"chat_id": AUTHORIZED_USER, "message_id": cb["message"]["message_id"], "reply_markup": {"inline_keyboard": []}})
                    elif "message" in upd and "text" in upd["message"]:
                        if upd["message"]["chat"]["id"] == AUTHORIZED_USER: process_command(AUTHORIZED_USER, upd["message"]["text"])
        except: time.sleep(5)

if __name__ == "__main__":
    Thread(target=run_server, daemon=True).start()
    Thread(target=auto_scanner, daemon=True).start()
    telegram()
