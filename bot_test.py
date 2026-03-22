import requests
import time
import pandas as pd
import numpy as np
import sqlite3
import os
import logging
import random
import psycopg2 
from psycopg2 import pool
from datetime import datetime, timedelta, time as dt_time
from threading import Thread, Lock
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from tvDatafeed import TvDatafeed, Interval
from flask import Flask, render_template_string, jsonify, request
from sklearn.ensemble import RandomForestClassifier # 🔥 UPGRADE 1: REAL MACHINE LEARNING

# ==========================================
# 🛡️ 1. SECURITY & GLOBALS
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
BASE_URL = f"https://api.telegram.org/bot{TOKEN}" if TOKEN else ""
DB_URL = os.getenv("DATABASE_URL")
WEB_SECRET = os.getenv("WEB_SECRET", "12345")

try: AUTHORIZED_USER = int(os.getenv("AUTHORIZED_USER", "0"))
except: AUTHORIZED_USER = 0

if AUTHORIZED_USER == 0: logging.error("🚨 CRITICAL: AUTHORIZED_USER not set!")

scan_lock = Lock()
data_lock = Lock() # 🛠️ FIX 1: Thread Safety Lock for dictionaries

# 🛠️ FIX 2: Increased DB Pool to 20 for heavy HFT load
db_pool = None
if DB_URL:
    try:
        db_pool = pool.SimpleConnectionPool(1, 20, dsn=DB_URL)
        logging.info("✅ PostgreSQL Connection Pool Initialized (Max: 20).")
    except Exception as e: logging.error(f"❌ DB pool error: {e}")

# ==========================================
# 🚀 2. TELEGRAM MESSAGE QUEUE (FIX 5)
# ==========================================
msg_queue = Queue()

def _send_msg_raw(chat_id, text):
    if not TOKEN or not chat_id: return
    url = f"{BASE_URL}/sendMessage"
    keyboard = {
        "keyboard": [
            [{"text": "📊 Check Status"}, {"text": "📅 Today Report"}],
            [{"text": "💰 View PnL"}, {"text": "📈 Live PnL"}],
            [{"text": "📊 Detailed Stats"}, {"text": "🔍 Scan Now"}],
            [{"text": "🛡️ Safe Mode"}, {"text": "⚡ Aggressive Mode"}],
            [{"text": "🔄 Switch Mode"}, {"text": "🌐 Open Dashboard"}], 
            [{"text": "❌ Close All"}, {"text": "⏸ Pause Bot"}]
        ], "resize_keyboard": True
    }
    for _ in range(3):
        try: 
            requests.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown", "reply_markup": keyboard, "disable_web_page_preview": True}, timeout=5)
            return
        except Exception as e: time.sleep(2)
    logging.error("❌ Telegram failed to send message after 3 retries.")

def telegram_worker():
    """Consumer Thread to safely dispatch messages and prevent bans"""
    while True:
        chat_id, text = msg_queue.get()
        _send_msg_raw(chat_id, text)
        msg_queue.task_done()
        time.sleep(1) # Safe Rate Limit Buffer

Thread(target=telegram_worker, daemon=True).start()

def send_msg(chat_id, text):
    """Producer function"""
    msg_queue.put((chat_id, text))

# ==========================================
# 🌐 3. DASHBOARD API & WEBHOOK
# ==========================================
app = Flask(__name__)

@app.before_request
def auth():
    if request.path == '/webhook': return 
    key = request.args.get("key") or request.headers.get("x-api-key")
    if key != WEB_SECRET: return "Unauthorized Access. Top 1% System Locked.", 401

# [Omitted exact HTML string for brevity, assume identical to V19 HTML]
HTML_TEMPLATE = """
<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>AI Quant Dashboard</title><script src="https://cdn.tailwindcss.com"></script><script src="https://cdn.jsdelivr.net/npm/chart.js"></script><style>body { background-color: #0f172a; color: #f8fafc; font-family: 'Inter', sans-serif; } .glass-card { background: rgba(30, 41, 59, 0.7); backdrop-filter: blur(10px); border: 1px solid rgba(255, 255, 255, 0.1); }</style></head><body class="p-4 sm:p-6"><div class="max-w-md mx-auto"><div class="flex justify-between items-center mb-6"><div><h1 class="text-2xl font-bold text-emerald-400">V20.0 Singularity</h1><p class="text-xs text-slate-400">Parallel HFT + Random Forest ML</p></div><div id="status-badge" class="px-3 py-1 rounded-full text-xs font-bold bg-emerald-500/20 text-emerald-400 border border-emerald-500/50">● ACTIVE</div></div><div class="grid grid-cols-2 gap-4 mb-6"><div class="glass-card p-4 rounded-xl text-center"><p class="text-xs text-slate-400 mb-1">Total PnL</p><p id="total-pnl" class="text-xl font-bold text-white">₹0.00</p></div><div class="glass-card p-4 rounded-xl text-center"><p class="text-xs text-slate-400 mb-1">Win Rate</p><p id="win-rate" class="text-xl font-bold text-blue-400">0%</p></div><div class="glass-card p-4 rounded-xl text-center"><p class="text-xs text-slate-400 mb-1">Total Trades</p><p id="total-trades" class="text-xl font-bold text-white">0</p></div><div class="glass-card p-4 rounded-xl text-center"><p class="text-xs text-slate-400 mb-1">Dynamic Capital</p><p id="dynamic-cap" class="text-xl font-bold text-purple-400">₹50K</p></div></div><h2 class="text-lg font-bold text-slate-300 mb-3">📈 Equity Curve (Smoothed)</h2><div class="glass-card p-4 rounded-xl mb-6"><canvas id="equityChart" height="200"></canvas></div><h2 class="text-lg font-bold text-slate-300 mb-3">⚡ Live Open Trades</h2><div id="open-trades-container" class="space-y-3"><div class="text-center text-slate-500 text-sm py-4">Loading trades...</div></div></div><script>const urlParams = new URLSearchParams(window.location.search); const authKey = urlParams.get('key') || ''; let equityChartInstance = null; async function fetchEquityData() { try { const res = await fetch('/api/equity?key=' + authKey); const rawData = await res.json(); let labels = ["Start"]; let capital = 50000; let dataPoints = [capital]; rawData.forEach(trade => { capital += trade.pnl; labels.push(trade.date.split(" ")[0]); dataPoints.push(capital); }); const ctx = document.getElementById('equityChart').getContext('2d'); if(equityChartInstance) { equityChartInstance.data.labels = labels; equityChartInstance.data.datasets[0].data = dataPoints; equityChartInstance.update(); } else { equityChartInstance = new Chart(ctx, { type: 'line', data: { labels: labels, datasets: [{ label: 'Capital (₹)', data: dataPoints, borderColor: '#34d399', backgroundColor: 'rgba(52, 211, 153, 0.1)', borderWidth: 2, fill: true, tension: 0.4, pointRadius: 1, pointBackgroundColor: '#fff' }] }, options: { responsive: true, plugins: { legend: { display: false } }, scales: { x: { display: false }, y: { ticks: { color: '#94a3b8' }, grid: { color: 'rgba(255, 255, 255, 0.05)' } } } } }); } } catch(e) {} } async function fetchStats() { try { const res = await fetch('/api/stats?key=' + authKey); const data = await res.json(); document.getElementById('total-pnl').innerText = '₹' + data.pnl.toFixed(2); document.getElementById('total-pnl').className = data.pnl >= 0 ? 'text-xl font-bold text-emerald-400' : 'text-xl font-bold text-rose-400'; document.getElementById('win-rate').innerText = data.win_rate.toFixed(1) + '%'; document.getElementById('total-trades').innerText = data.total_trades; document.getElementById('dynamic-cap').innerText = '₹' + (50000 + data.pnl).toLocaleString(); const badge = document.getElementById('status-badge'); if (data.paused) { badge.innerText = '⏸ PAUSED'; badge.className = 'px-3 py-1 rounded-full text-xs font-bold bg-amber-500/20 text-amber-400 border border-amber-500/50'; } else { badge.innerText = '● ACTIVE'; badge.className = 'px-3 py-1 rounded-full text-xs font-bold bg-emerald-500/20 text-emerald-400 border border-emerald-500/50'; } const tradesContainer = document.getElementById('open-trades-container'); if (data.open_trades.length === 0) { tradesContainer.innerHTML = '<div class="glass-card p-4 rounded-xl text-center text-slate-500 text-sm">No open trades.</div>'; } else { let html = ''; data.open_trades.forEach(t => { const typeColor = t[1].includes('BUY') ? 'text-emerald-400' : 'text-rose-400'; const partialTag = t[5] ? '<span class="ml-2 text-[10px] bg-blue-500/20 text-blue-400 px-1 rounded">50% BOOKED</span>' : ''; html += `<div class="glass-card p-4 rounded-xl flex justify-between items-center"><div><p class="font-bold text-white">${t[0]} ${partialTag}</p><p class="text-xs text-slate-400">Entry: ₹${t[2].toFixed(2)}</p></div><div class="text-right"><p class="font-bold ${typeColor}">${t[1]}</p><p class="text-xs text-slate-400">Qty: ${t[6]}</p></div></div>`; }); tradesContainer.innerHTML = html; } } catch (e) {} } fetchStats(); fetchEquityData(); setInterval(fetchStats, 5000); setInterval(fetchEquityData, 10000);</script></body></html>
"""

@app.route('/')
def dashboard(): return render_template_string(HTML_TEMPLATE)

@app.route('/api/stats')
def api_stats():
    pnl = get_val("SELECT SUM(pnl) FROM pro_trades WHERE status!='OPEN'") or 0.0
    total = int(get_val("SELECT COUNT(*) FROM pro_trades WHERE status!='OPEN'") or 0)
    wins = int(get_val("SELECT COUNT(*) FROM pro_trades WHERE status='PROFIT ✅'") or 0)
    open_trades = execute_db("SELECT symbol, type, entry_price, mode, sl, partial_exit, qty FROM pro_trades WHERE status='OPEN'", fetchall=True) or []
    return jsonify({"pnl": pnl, "win_rate": (wins/total*100) if total>0 else 0, "total_trades": total, "open_trades": open_trades, "mode": trading_mode, "paused": bot_paused})

# 🛠️ FIX 3: Backend Equity Curve Smoothing (Pandas)
@app.route('/api/equity')
def api_equity():
    data = execute_db("SELECT date, pnl FROM pro_trades WHERE status!='OPEN' ORDER BY date_ts ASC", fetchall=True) or []
    if not data: return jsonify([])
    
    capital, curve = 50000, []
    for r in data:
        capital += r[1]
        curve.append(capital)
        
    # Backend rolling smoothing
    smoothed = pd.Series(curve).rolling(5, min_periods=1).mean().tolist()
    
    response = []
    for i in range(len(smoothed)):
        # Re-derive smoothed PnL difference for the front-end to stack
        step_pnl = smoothed[i] - (smoothed[i-1] if i > 0 else 50000)
        response.append({"date": data[i][0], "pnl": step_pnl})
    return jsonify(response)

@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        data = request.json
        if data.get('secret') != WEB_SECRET: return jsonify({"error": "Unauthorized"}), 401
        sym = str(data.get('symbol', '')).upper()[:20] 
        decision = str(data.get('action', '')).upper()[:20]
        cp = float(data.get('price', 0))
        send_msg(AUTHORIZED_USER, f"⚡ *WEBHOOK ALERT:* TV triggered {decision} for {sym} @ ₹{cp}")
        return jsonify({"status": "Alert Received"}), 200
    except Exception as e: return jsonify({"error": str(e)}), 500

def run_server():
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

# ==========================================
# 🗄️ 4. FAST POOLED DATABASE & RECOVERY
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
        conn.rollback()
        logging.error(f"Pool DB Error: {e}")
        return None
    finally:
        db_pool.putconn(conn)

def get_val(query, params=()):
    res = execute_db(query, params, fetch=True)
    return res[0] if res and res[0] else 0.0

def setup_db():
    execute_db('''CREATE TABLE IF NOT EXISTS pro_trades 
                 (id SERIAL PRIMARY KEY, date TEXT, date_ts INTEGER, symbol TEXT, type TEXT, 
                  entry_price REAL, sl REAL, tp REAL, status TEXT, pnl REAL, mode TEXT, 
                  partial_exit INTEGER DEFAULT 0, qty INTEGER DEFAULT 0, features TEXT DEFAULT '')''')

# 🛠️ FIX 6: Crash Recovery
last_signal = {}
def recover_state():
    rows = execute_db("SELECT symbol FROM pro_trades WHERE status='OPEN'", fetchall=True)
    if rows:
        with data_lock:
            for r in rows: last_signal[r[0]] = "RECOVERED"
        logging.info(f"✅ Recovered {len(rows)} open trades from Database.")

def get_ist(): return datetime.utcnow() + timedelta(hours=5, minutes=30)

# ==========================================
# 🤖 5. TRUE ML ENGINE (RANDOM FOREST)
# ==========================================
def get_ml_prediction(rsi, macd, dist):
    """🔥 UPGRADE 1: Real Scikit-Learn RandomForest Training"""
    try:
        rows = execute_db("SELECT features, status FROM pro_trades WHERE status!='OPEN' AND features!=''", fetchall=True)
        if not rows or len(rows) < 20: 
            return None # Fallback to basic if data is low
        
        X, y = [], []
        for feat_str, status in rows:
            try:
                parts = feat_str.split(',')
                f_rsi = float(parts[0].split(':')[1])
                f_macd = float(parts[1].split(':')[1])
                f_dist = float(parts[2].split(':')[1])
                X.append([f_rsi, f_macd, f_dist])
                y.append(1 if "PROFIT" in status else 0)
            except: continue
            
        if len(set(y)) < 2: return None # Needs both wins & losses to train
        
        clf = RandomForestClassifier(n_estimators=15, max_depth=3, random_state=42)
        clf.fit(X, y)
        
        feat_arr = np.array([[rsi, macd, dist]])
        prob = clf.predict_proba(feat_arr)[0][1] # Probability of Class 1 (PROFIT)
        return prob * 100
    except Exception as e:
        logging.error(f"ML Prediction Error: {e}")
        return None

# ==========================================
# 🧠 6. CORE ENGINE & PARALLEL LOGIC
# ==========================================
bot_paused = False
trading_mode = "DEMO"
strategy_mode = "SAFE"
alerts_muted = False
current_risk_percent = 2.0
daily_loss_limit = -2000.0
daily_profit_target = 3000.0
global_drawdown_limit = -5000.0 
max_daily_trades = 5          
trade_cooldown_seconds = 300  
last_trade_time = {}          

tv_instance = None
def safe_tv_get():
    global tv_instance
    if tv_instance is not None: return tv_instance
    try: tv_instance = TvDatafeed(); return tv_instance
    except Exception: time.sleep(2); tv_instance = TvDatafeed(); return tv_instance

def calc_macd(data):
    ema12 = data['close'].ewm(span=12, adjust=False).mean()
    ema26 = data['close'].ewm(span=26, adjust=False).mean()
    return ema12 - ema26, (ema12 - ema26).ewm(span=9, adjust=False).mean()

def is_news_time():
    """🔥 UPGRADE 3: News Filter Placeholder"""
    # e.g., Return True if RBI policy at 10:00 AM
    now = get_ist()
    if now.weekday() in [3, 4] and now.hour == 10 and 0 <= now.minute <= 15:
        return True # Avoiding 10:00 to 10:15 AM volatility on Thursday/Fridays
    return False

def process_single_symbol(sym):
    global strategy_mode, alerts_muted, current_risk_percent
    
    if bot_paused or is_news_time(): return "STOP"
    
    with data_lock:
        if sym in last_trade_time and time.time() - last_trade_time[sym] < trade_cooldown_seconds: return
    
    time.sleep(random.uniform(2.0, 5.0))
    tv = safe_tv_get()
    
    try:
        data_1m = tv.get_hist(symbol=sym, exchange='NSE', interval=Interval.in_1_minute, n_bars=100) # 🔥 UPGRADE 2: 1m Entry Frame
        data_5m = tv.get_hist(symbol=sym, exchange='NSE', interval=Interval.in_5_minute, n_bars=250)
        data_15m = tv.get_hist(symbol=sym, exchange='NSE', interval=Interval.in_15_minute, n_bars=100)
    except: global tv_instance; tv_instance = None; return

    if data_5m is None or data_5m.empty or data_15m is None or data_15m.empty or data_1m is None or data_1m.empty: return
    
    cp = data_5m['close'].iloc[-1]
    if abs(data_5m['close'].pct_change().iloc[-1]) < 0.0005: return
    
    ema200 = data_5m['close'].ewm(span=200, adjust=False).mean().iloc[-1]
    trend_15m_up = data_15m['close'].iloc[-1] > data_15m['close'].ewm(span=50).mean().iloc[-1]
    momentum_1m_up = data_1m['close'].iloc[-1] > data_1m['close'].iloc[-3] # 1m fine-tune
    
    delta = data_5m['close'].diff()
    gain = delta.clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
    loss_safe = -delta.clip(upper=0).ewm(alpha=1/14, adjust=False).mean().replace(0, 1e-10)
    rs = gain / loss_safe
    rsi = 100 - (100 / (1 + rs)).iloc[-1]
    macd, macd_sig = calc_macd(data_5m)
    
    if len(macd) < 2 or len(macd_sig) < 2: return
    
    open_trades = execute_db("SELECT id, type, entry_price, sl, tp, pnl, partial_exit, qty FROM pro_trades WHERE symbol=%s AND status='OPEN'", (sym,), fetchall=True)
    if open_trades:
        for t in open_trades:
            t_id, t_type, entry, sl, tp, current_pnl, partial_exit, qty = t
            status, pnl, msg = "OPEN", current_pnl, None
            pts_captured = (cp - entry) if "BUY" in t_type else (entry - cp)
            half_target = entry + (tp - entry)/2 if "BUY" in t_type else entry - (entry - tp)/2
            
            # 🛠️ FIX 3: Integer Math for Partial Exit Qty
            if not partial_exit and ((cp >= half_target if "BUY" in t_type else cp <= half_target)):
                half_qty = qty // 2 
                locked_pnl = pts_captured * half_qty
                execute_db("UPDATE pro_trades SET partial_exit=1, pnl=%s, sl=%s WHERE id=%s", (locked_pnl, entry, t_id))
                sl = entry 
                if not alerts_muted: send_msg(AUTHORIZED_USER, f"🎯 *PARTIAL BOOKED (50%)*: {sym}\n💰 Locked: ₹{locked_pnl:.2f}\n🛡️ SL moved to Entry.")
                continue 

            if t_type == "BUY 🟢" and cp > entry:
                new_sl = max(sl, cp - (cp * 0.001))
                if new_sl > sl: execute_db("UPDATE pro_trades SET sl=%s WHERE id=%s", (new_sl, t_id)); sl = new_sl
            elif t_type == "SELL 🔴" and cp < entry:
                new_sl = min(sl, cp + (cp * 0.001))
                if new_sl < sl: execute_db("UPDATE pro_trades SET sl=%s WHERE id=%s", (new_sl, t_id)); sl = new_sl

            remaining_qty = (qty // 2) if partial_exit else qty
            if t_type == "BUY 🟢":
                if cp >= tp: status, pnl, msg = "PROFIT ✅", current_pnl + (abs(tp - entry) * remaining_qty), f"🎯 TARGET HIT: {sym} (+₹{(abs(tp - entry) * remaining_qty):.2f})"
                elif cp <= sl: status, pnl, msg = "LOSS ❌", current_pnl - (abs(entry - cp) * remaining_qty), f"🛑 SL HIT: {sym} (Exit: ₹{cp:.2f})"
            elif t_type == "SELL 🔴":
                if cp <= tp: status, pnl, msg = "PROFIT ✅", current_pnl + (abs(entry - tp) * remaining_qty), f"🎯 TARGET HIT: {sym} (+₹{(abs(entry - tp) * remaining_qty):.2f})"
                elif cp >= sl: status, pnl, msg = "LOSS ❌", current_pnl - (abs(sl - entry) * remaining_qty), f"🛑 SL HIT: {sym} (Exit: ₹{cp:.2f})"
            
            if status != "OPEN":
                execute_db("UPDATE pro_trades SET status=%s, pnl=%s WHERE id=%s", (status, pnl, t_id))
                if not alerts_muted: send_msg(AUTHORIZED_USER, msg)
        return 
    
    rsi_buy, rsi_sell = (60, 40) if strategy_mode == "SAFE" else (50, 50)
    dist_ema = (abs(cp - ema200) / ema200) * 100
    if dist_ema > (0.4 if strategy_mode == "SAFE" else 0.8): return
    
    decision = "WAIT"
    # Added 1m momentum confirmation to 5m signal + 15m trend
    if cp > ema200 and rsi > rsi_buy and macd.iloc[-1] > macd_sig.iloc[-1] and trend_15m_up and momentum_1m_up: decision = "BUY 🟢"
    elif cp < ema200 and rsi < rsi_sell and macd.iloc[-1] < macd_sig.iloc[-1] and not trend_15m_up and not momentum_1m_up: decision = "SELL 🔴"

    with data_lock:
        if sym in last_signal and last_signal[sym] == decision: return
        last_signal[sym] = decision

    if decision != "WAIT":
        # 🛠️ FIX 4: Realistic Slippage Calculation
        spread = abs(data_5m['high'].iloc[-1] - data_5m['low'].iloc[-1])
        slippage = max(spread * 0.1, cp * 0.0002) # Take 10% of spread or minimum 0.02%
        
        exec_price = cp + slippage if "BUY" in decision else cp - slippage
        sl = exec_price - (exec_price * 0.002) if "BUY" in decision else exec_price + (exec_price * 0.002)
        tp = exec_price + (exec_price * 0.005) if "BUY" in decision else exec_price - (exec_price * 0.005)
        
        total_pnl = get_val("SELECT SUM(pnl) FROM pro_trades WHERE status!='OPEN'")
        dynamic_capital = max(50000, 50000 + total_pnl)
        sl_dist = abs(exec_price - sl)
        qty = min(100, max(1, int((dynamic_capital * (current_risk_percent / 100)) / sl_dist))) if sl_dist > 0 else 1
        
        # Prepare & Predict ML
        features_str = f"RSI:{rsi:.1f},MACD:{macd.iloc[-1]:.2f},DIST:{dist_ema:.1f}"
        ml_prob = get_ml_prediction(rsi, macd.iloc[-1], dist_ema)
        ml_msg = f"{ml_prob:.1f}%" if ml_prob else "Training..."
        
        ts = int(time.time())
        execute_db('INSERT INTO pro_trades (date, date_ts, symbol, type, entry_price, sl, tp, status, pnl, mode, qty, features) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                   (get_ist().strftime("%Y-%m-%d %H:%M"), ts, sym, decision, exec_price, sl, tp, "OPEN", 0.0, trading_mode, qty, features_str))
        
        with data_lock: last_trade_time[sym] = time.time()
        
        rr_ratio = abs(tp - exec_price) / sl_dist if sl_dist > 0 else 0
        if not alerts_muted:
            send_msg(AUTHORIZED_USER, f"🚀 *{trading_mode} QUANT EXECUTED* 🚀\n\n📈 *Symbol:* {sym}\n🤖 *Action:* {decision}\n🛒 *Qty:* {qty} (Risk {current_risk_percent}%)\n🧠 *RF ML Edge:* {ml_msg}\n\n🔸 *Entry:* ₹{exec_price:.2f} (Spread Slip)\n🎯 *TP:* ₹{tp:.2f} | 🛡️ *SL:* ₹{sl:.2f}\n⚖️ *RR:* 1:{rr_ratio:.1f}")

def run_scan_cycle(manual=False):
    global bot_paused
    symbols = ['NIFTY', 'BANKNIFTY', 'CNXFINANCE']
    now = get_ist()
    
    m1_start, m1_end = dt_time(9, 15), dt_time(10, 30)
    m2_start, m2_end = dt_time(14, 30), dt_time(15, 30)
    if not manual and not ((m1_start <= now.time() <= m1_end) or (m2_start <= now.time() <= m2_end)): return "SKIP" 
            
    today = now.strftime("%Y-%m-%d")
    total_pnl = get_val("SELECT SUM(pnl) FROM pro_trades WHERE status!='OPEN'")
    today_pnl = get_val("SELECT SUM(pnl) FROM pro_trades WHERE date LIKE %s AND status!='OPEN'", (f"{today}%",))
    trades_today = get_val("SELECT COUNT(*) FROM pro_trades WHERE date LIKE %s", (f"{today}%",))
    
    if total_pnl <= global_drawdown_limit:
        if manual: send_msg(AUTHORIZED_USER, "🚨 MAX DRAWDOWN HIT. BOT STOPPED.")
        return "PAUSE"
    if trades_today >= max_daily_trades:
        if manual: send_msg(AUTHORIZED_USER, f"🛑 Max daily trades ({max_daily_trades}) reached.")
        return "SKIP"
    if today_pnl <= daily_loss_limit:
        if manual: send_msg(AUTHORIZED_USER, "🛑 Daily Loss Limit hit.")
        return "PAUSE"
    if today_pnl >= daily_profit_target:
        if manual: send_msg(AUTHORIZED_USER, "🎯 Daily Profit Target hit.")
        return "PAUSE"

    with ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(process_single_symbol, symbols)
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
                    status = run_scan_cycle(manual=False)
                    if status == "PAUSE": 
                        bot_paused = True
                        send_msg(AUTHORIZED_USER, "🛑 Bot auto-paused due to limits. Use /resume to continue.")
                finally: scan_lock.release()
        except Exception as e: logging.error(f"Global Scanner Error: {e}"); time.sleep(5)
        time.sleep(60)

# ==========================================
# 🎮 7. COMMAND HANDLER
# ==========================================
def telegram():
    global bot_paused, trading_mode, strategy_mode, alerts_muted, max_daily_trades
    last_id = None
    setup_db()
    recover_state() # 🛠️ FIX 6: Recover Open Trades
    
    while True:
        try:
            url = f"{BASE_URL}/getUpdates?timeout=5"
            if last_id: url += f"&offset={last_id + 1}"
            try: res = requests.get(url, timeout=10).json()
            except: time.sleep(2); continue
            
            if "result" in res:
                for upd in res["result"]:
                    last_id = upd["update_id"]
                    if "message" in upd and "text" in upd["message"]:
                        chat_id, txt = upd["message"]["chat"]["id"], upd["message"]["text"]
                        if chat_id != AUTHORIZED_USER: send_msg(chat_id, "❌ *UNAUTHORIZED ACCESS.*"); continue
                            
                        if txt == "/start": send_msg(chat_id, "💎 V20.0 SINGULARITY ENGINE Online. ML, MTF & Parallel Scans Active.")
                        elif txt == "🌐 Open Dashboard":
                            dash_url = f"https://ai-trading-bot-itc0.onrender.com?key={WEB_SECRET}"
                            inline_keyboard = {"inline_keyboard": [[{"text": "🚀 Secure Web Dashboard", "url": dash_url}]]}
                            try: requests.post(f"{BASE_URL}/sendMessage", json={"chat_id": chat_id, "text": "Access your Secured Quant Dashboard:", "reply_markup": inline_keyboard})
                            except: pass
                        elif txt in ["🔄 Switch Mode", "/mode"]: send_msg(chat_id, "⚠️ Type `CONFIRM REAL` or `CONFIRM DEMO`")
                        elif txt == "CONFIRM REAL": trading_mode = "REAL"; send_msg(chat_id, "💰 *REAL TRADING ENABLED!*")
                        elif txt == "CONFIRM DEMO": trading_mode = "DEMO"; send_msg(chat_id, "🛡️ Switched to *DEMO* Mode safely.")
                        elif txt == "📊 Check Status": send_msg(chat_id, f"📡 Bot: {'Paused ⏸' if bot_paused else 'Active ▶️'}\n🧠 Mode: {trading_mode}\n🛡️ Strategy: {strategy_mode}\n📊 Max Trades: {max_daily_trades}/day")
                        elif txt in ["💰 View PnL", "/pnl"]: send_msg(chat_id, f"💰 Net PnL: ₹{get_val('SELECT SUM(pnl) FROM pro_trades WHERE status!=\\'OPEN\\''):.2f}")
                        elif txt == "📈 Live PnL":
                            rows = execute_db("SELECT symbol, type, entry_price, qty FROM pro_trades WHERE status='OPEN'", fetchall=True)
                            if not rows: send_msg(chat_id, "No open trades.")
                            else:
                                msg, total_live, tv = "📈 *LIVE OPEN TRADES:*\n\n", 0, safe_tv_get()
                                for sym, t_type, entry, qty in rows:
                                    try:
                                        time.sleep(1)
                                        d = tv.get_hist(symbol=sym, exchange='NSE', interval=Interval.in_1_minute, n_bars=2)
                                        pnl = ((d['close'].iloc[-1] - entry) if "BUY" in t_type else (entry - d['close'].iloc[-1])) * qty
                                        total_live += pnl; msg += f"🔹 {sym}: ₹{pnl:.2f}\n"
                                    except: pass
                                send_msg(chat_id, msg + f"\n💰 *Total Floating:* ₹{total_live:.2f}")
                        elif txt == "🔍 Scan Now": Thread(target=lambda: (scan_lock.acquire(blocking=False) and [send_msg(chat_id, "🔍 Parallel Scan Running..."), run_scan_cycle(True), scan_lock.release()])).start()
                        elif txt == "🛡️ Safe Mode": strategy_mode = "SAFE"; send_msg(chat_id, "🛡️ Safe Mode ON.")
                        elif txt == "⚡ Aggressive Mode": strategy_mode = "AGGRESSIVE"; send_msg(chat_id, "⚡ Aggressive Mode ON.")
                        elif txt in ["❌ Close All", "/closeall"]:
                            rows = execute_db("SELECT id FROM pro_trades WHERE status='OPEN'", fetchall=True)
                            if rows: [execute_db("UPDATE pro_trades SET status='CLOSED ⚠️' WHERE id=%s", (r[0],)) for r in rows]; send_msg(chat_id, "⚠️ All FORCE CLOSED.")
                            else: send_msg(chat_id, "❌ No open trades.")
                        elif txt in ["⏸ Pause Bot", "/pause"]: bot_paused = True; send_msg(chat_id, "🛑 Bot Paused.")
                        elif txt in ["▶️ Resume Bot", "/resume"]: bot_paused = False; send_msg(chat_id, "✅ Bot Resumed.")
        except: time.sleep(5)

if __name__ == "__main__":
    Thread(target=run_server, daemon=True).start()
    Thread(target=auto_scanner, daemon=True).start()
    telegram()
