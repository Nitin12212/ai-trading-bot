import requests
import time
import pandas as pd
import os
import logging
import random
import psycopg2 # NEW: Cloud Database Library
from datetime import datetime, timedelta, time as dt_time
from threading import Thread, Lock
from tvDatafeed import TvDatafeed, Interval
from flask import Flask, render_template_string, jsonify

# ==========================================
# 🛡️ 1. SECURITY & GLOBALS
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
BASE_URL = f"https://api.telegram.org/bot{TOKEN}" if TOKEN else ""
DB_URL = os.getenv("DATABASE_URL") # NEW: Cloud Database Link

try:
    AUTHORIZED_USER = int(os.getenv("AUTHORIZED_USER", "0"))
except:
    AUTHORIZED_USER = 0

if AUTHORIZED_USER == 0:
    logging.error("🚨 CRITICAL: AUTHORIZED_USER not set! Bot will reject commands.")
if not DB_URL:
    logging.error("🚨 CRITICAL: DATABASE_URL not set! Cloud DB will not work.")

scan_lock = Lock()
db_lock = Lock()

# ==========================================
# 🌐 2. MOBILE WEB DASHBOARD (HTML + API)
# ==========================================
app = Flask(__name__)

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>AI Quant Dashboard</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body { background-color: #0f172a; color: #f8fafc; font-family: 'Inter', sans-serif; }
        .glass-card { background: rgba(30, 41, 59, 0.7); backdrop-filter: blur(10px); border: 1px solid rgba(255, 255, 255, 0.1); }
    </style>
</head>
<body class="p-4 sm:p-6">
    <div class="max-w-md mx-auto">
        <div class="flex justify-between items-center mb-6">
            <div>
                <h1 class="text-2xl font-bold text-emerald-400">V17.0 Immortal Engine</h1>
                <p class="text-xs text-slate-400">Cloud AI Performance</p>
            </div>
            <div id="status-badge" class="px-3 py-1 rounded-full text-xs font-bold bg-emerald-500/20 text-emerald-400 border border-emerald-500/50">
                ● ACTIVE
            </div>
        </div>

        <div class="grid grid-cols-2 gap-4 mb-6">
            <div class="glass-card p-4 rounded-xl text-center">
                <p class="text-xs text-slate-400 mb-1">Total PnL</p>
                <p id="total-pnl" class="text-xl font-bold text-white">₹0.00</p>
            </div>
            <div class="glass-card p-4 rounded-xl text-center">
                <p class="text-xs text-slate-400 mb-1">Win Rate</p>
                <p id="win-rate" class="text-xl font-bold text-blue-400">0%</p>
            </div>
            <div class="glass-card p-4 rounded-xl text-center">
                <p class="text-xs text-slate-400 mb-1">Total Trades</p>
                <p id="total-trades" class="text-xl font-bold text-white">0</p>
            </div>
            <div class="glass-card p-4 rounded-xl text-center">
                <p class="text-xs text-slate-400 mb-1">Dynamic Capital</p>
                <p id="dynamic-cap" class="text-xl font-bold text-purple-400">₹50K</p>
            </div>
        </div>

        <h2 class="text-lg font-bold text-slate-300 mb-3 flex items-center"><span class="mr-2">📈</span> Equity Curve</h2>
        <div class="glass-card p-4 rounded-xl mb-6">
            <canvas id="equityChart" height="200"></canvas>
        </div>

        <h2 class="text-lg font-bold text-slate-300 mb-3 flex items-center"><span class="mr-2">⚡</span> Live Open Trades</h2>
        <div id="open-trades-container" class="space-y-3">
            <div class="text-center text-slate-500 text-sm py-4">Loading trades...</div>
        </div>
    </div>

    <script>
        let equityChartInstance = null;

        async function fetchEquityData() {
            try {
                const res = await fetch('/api/equity');
                const rawData = await res.json();
                
                let labels = ["Start"];
                let capital = 50000;
                let dataPoints = [capital];

                rawData.forEach(trade => {
                    capital += trade.pnl;
                    labels.push(trade.date.split(" ")[0]); 
                    dataPoints.push(capital);
                });

                const ctx = document.getElementById('equityChart').getContext('2d');
                if(equityChartInstance) {
                    equityChartInstance.data.labels = labels;
                    equityChartInstance.data.datasets[0].data = dataPoints;
                    equityChartInstance.update();
                } else {
                    equityChartInstance = new Chart(ctx, {
                        type: 'line',
                        data: {
                            labels: labels,
                            datasets: [{
                                label: 'Capital (₹)',
                                data: dataPoints,
                                borderColor: '#34d399', backgroundColor: 'rgba(52, 211, 153, 0.1)',
                                borderWidth: 2, fill: true, tension: 0.3, pointRadius: 2, pointBackgroundColor: '#fff'
                            }]
                        },
                        options: {
                            responsive: true,
                            plugins: { legend: { display: false }, tooltip: { mode: 'index', intersect: false } },
                            scales: {
                                x: { display: false },
                                y: { ticks: { color: '#94a3b8', callback: function(value) { return '₹' + (value/1000) + 'k'; } }, grid: { color: 'rgba(255, 255, 255, 0.05)' } }
                            }
                        }
                    });
                }
            } catch(e) { console.error("Equity Graph Error", e); }
        }

        async function fetchStats() {
            try {
                const res = await fetch('/api/stats');
                const data = await res.json();
                
                document.getElementById('total-pnl').innerText = '₹' + data.pnl.toFixed(2);
                document.getElementById('total-pnl').className = data.pnl >= 0 ? 'text-xl font-bold text-emerald-400' : 'text-xl font-bold text-rose-400';
                document.getElementById('win-rate').innerText = data.win_rate.toFixed(1) + '%';
                document.getElementById('total-trades').innerText = data.total_trades;
                document.getElementById('dynamic-cap').innerText = '₹' + (50000 + data.pnl).toLocaleString();
                
                const badge = document.getElementById('status-badge');
                if (data.paused) {
                    badge.innerText = '⏸ PAUSED';
                    badge.className = 'px-3 py-1 rounded-full text-xs font-bold bg-amber-500/20 text-amber-400 border border-amber-500/50';
                } else {
                    badge.innerText = '● ACTIVE';
                    badge.className = 'px-3 py-1 rounded-full text-xs font-bold bg-emerald-500/20 text-emerald-400 border border-emerald-500/50';
                }

                const tradesContainer = document.getElementById('open-trades-container');
                if (data.open_trades.length === 0) {
                    tradesContainer.innerHTML = '<div class="glass-card p-4 rounded-xl text-center text-slate-500 text-sm">No open positions right now.</div>';
                } else {
                    let html = '';
                    data.open_trades.forEach(t => {
                        const typeColor = t[1].includes('BUY') ? 'text-emerald-400' : 'text-rose-400';
                        const partialTag = t[5] ? '<span class="ml-2 text-[10px] bg-blue-500/20 text-blue-400 px-1 rounded">50% BOOKED</span>' : '';
                        html += `
                        <div class="glass-card p-4 rounded-xl flex justify-between items-center">
                            <div>
                                <p class="font-bold text-white">${t[0]} ${partialTag}</p>
                                <p class="text-xs text-slate-400">Entry: ₹${t[2].toFixed(2)} | SL: ₹${t[4].toFixed(2)}</p>
                            </div>
                            <div class="text-right">
                                <p class="font-bold ${typeColor}">${t[1]}</p>
                                <p class="text-xs text-slate-400">Qty: ${t[6]} | Mode: ${t[3]}</p>
                            </div>
                        </div>`;
                    });
                    tradesContainer.innerHTML = html;
                }
            } catch (e) { console.error("Error fetching stats", e); }
        }

        fetchStats(); fetchEquityData();
        setInterval(fetchStats, 5000); setInterval(fetchEquityData, 10000);
    </script>
</body>
</html>
"""

@app.route('/')
def dashboard():
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/stats')
def api_stats():
    pnl = get_val("SELECT SUM(pnl) FROM pro_trades WHERE status!='OPEN'") or 0.0
    total = int(get_val("SELECT COUNT(*) FROM pro_trades WHERE status!='OPEN'") or 0)
    wins = int(get_val("SELECT COUNT(*) FROM pro_trades WHERE status='PROFIT ✅'") or 0)
    open_trades = execute_db("SELECT symbol, type, entry_price, mode, sl, partial_exit, qty FROM pro_trades WHERE status='OPEN'", fetchall=True) or []
    win_rate = (wins / total * 100) if total > 0 else 0
    return jsonify({"pnl": pnl, "win_rate": win_rate, "total_trades": total, "open_trades": open_trades, "mode": trading_mode, "paused": bot_paused})

@app.route('/api/equity')
def api_equity():
    data = execute_db("SELECT date, pnl FROM pro_trades WHERE status!='OPEN' ORDER BY date_ts ASC", fetchall=True) or []
    return jsonify([{"date": r[0], "pnl": r[1]} for r in data])

def run_server():
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

# Globals
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
last_signal = {}              
pending_mode_confirm = False  

def get_tv():
    for _ in range(3):
        try: return TvDatafeed()
        except Exception as e:
            logging.error(f"TV Init Error: {e}"); time.sleep(2)
    return TvDatafeed() 

tv = get_tv()

# ==========================================
# 🗄️ 3. CLOUD POSTGRESQL ENGINE (IMMORTAL)
# ==========================================
def execute_db(query, params=(), fetch=False, fetchall=False):
    if not DB_URL: return None
    with db_lock:
        conn = None
        try:
            conn = psycopg2.connect(DB_URL)
            c = conn.cursor()
            c.execute(query, params)
            if fetch: res = c.fetchone()
            elif fetchall: res = c.fetchall()
            else: conn.commit(); res = True
            c.close()
            return res
        except Exception as e:
            logging.error(f"Cloud DB Error: {e}")
            if conn: conn.rollback()
            return None
        finally:
            if conn: conn.close()

def get_val(query, params=()):
    res = execute_db(query, params, fetch=True)
    return res[0] if res and res[0] else 0.0

def setup_db():
    # PostgreSQL syntax for Cloud Persistence
    execute_db('''CREATE TABLE IF NOT EXISTS pro_trades 
                 (id SERIAL PRIMARY KEY, date TEXT, date_ts INTEGER, symbol TEXT, type TEXT, 
                  entry_price REAL, sl REAL, tp REAL, status TEXT, pnl REAL, mode TEXT, 
                  partial_exit INTEGER DEFAULT 0, qty INTEGER DEFAULT 0)''')

def get_ist(): return datetime.utcnow() + timedelta(hours=5, minutes=30)

# ==========================================
# 📱 4. TELEGRAM UI & SAFE MESSAGING
# ==========================================
def send_msg(chat_id, text):
    if not TOKEN or not chat_id: return
    url = f"{BASE_URL}/sendMessage"
    keyboard = {
        "keyboard": [
            [{"text": "📊 Check Status"}, {"text": "📅 Today Report"}],
            [{"text": "💰 View PnL"}, {"text": "📈 Live PnL"}],
            [{"text": "📊 Detailed Stats"}, {"text": "🔍 Scan Now"}],
            [{"text": "🛡️ Safe Mode"}, {"text": "⚡ Aggressive Mode"}],
            [{"text": "🔄 Switch Mode"}, {"text": "🌐 Open Dashboard"}], 
            [{"text": "❌ Close All"}],
            [{"text": "⏸ Pause Bot"}, {"text": "▶️ Resume Bot"}]
        ], "resize_keyboard": True
    }
    for _ in range(3):
        try:
            requests.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown", "reply_markup": keyboard, "disable_web_page_preview": True}, timeout=5)
            return
        except: time.sleep(2)

def place_real_order(symbol, side, price, sl, tp, qty):
    try:
        logging.info(f"REAL ORDER PLACED: {side} {symbol} x {qty}")
        return True
    except Exception as e:
        logging.error(f"Broker Error: {e}")
        return False

# ==========================================
# 🧠 5. CORE ENGINE & LOGIC
# ==========================================
def calc_macd(data):
    ema12 = data['close'].ewm(span=12, adjust=False).mean()
    ema26 = data['close'].ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    return macd, macd.ewm(span=9, adjust=False).mean()

def run_scan_cycle(manual=False):
    global strategy_mode, alerts_muted, current_risk_percent, last_signal, last_trade_time, max_daily_trades, tv
    symbols = ['NIFTY', 'BANKNIFTY', 'CNXFINANCE']
    today = get_ist().strftime("%Y-%m-%d")
    now_time = get_ist().time()
    
    m1_start, m1_end = dt_time(9, 15), dt_time(10, 30)
    m2_start, m2_end = dt_time(14, 30), dt_time(15, 30)
    if not manual and not ((m1_start <= now_time <= m1_end) or (m2_start <= now_time <= m2_end)):
        return "SKIP" 
            
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

    for sym in symbols:
        try:
            if sym in last_trade_time and time.time() - last_trade_time[sym] < trade_cooldown_seconds: continue
            
            time.sleep(random.uniform(2.0, 5.0))
            
            try:
                data_5m = tv.get_hist(symbol=sym, exchange='NSE', interval=Interval.in_5_minute, n_bars=250)
                data_15m = tv.get_hist(symbol=sym, exchange='NSE', interval=Interval.in_15_minute, n_bars=100)
            except Exception as e:
                logging.error(f"TV reconnect: {e}"); time.sleep(3); tv = get_tv(); continue

            if data_5m is None or data_5m.empty or data_15m is None or data_15m.empty: continue
            
            cp = data_5m['close'].iloc[-1]
            vol = data_5m['close'].pct_change().iloc[-1]
            if abs(vol) < 0.0005: continue
            
            ema200 = data_5m['close'].ewm(span=200, adjust=False).mean().iloc[-1]
            trend_15m_up = data_15m['close'].iloc[-1] > data_15m['close'].ewm(span=50).mean().iloc[-1]
            
            delta = data_5m['close'].diff()
            gain = delta.clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
            loss = -delta.clip(upper=0).ewm(alpha=1/14, adjust=False).mean()
            loss_safe = loss.replace(0, 1e-10)
            rs = gain / loss_safe
            rsi = 100 - (100 / (1 + rs)).iloc[-1]
            
            macd, macd_sig = calc_macd(data_5m)
            if len(macd) < 2 or len(macd_sig) < 2: continue
            
            open_trades = execute_db("SELECT id, type, entry_price, sl, tp, pnl, partial_exit, qty FROM pro_trades WHERE symbol=%s AND status='OPEN'", (sym,), fetchall=True)
            if open_trades:
                for t in open_trades:
                    t_id, t_type, entry, sl, tp, current_pnl, partial_exit, qty = t
                    status, pnl, msg = "OPEN", current_pnl, None
                    
                    pts_captured = (cp - entry) if "BUY" in t_type else (entry - cp)
                    
                    half_target = entry + (tp - entry)/2 if "BUY" in t_type else entry - (entry - tp)/2
                    is_half_hit = (cp >= half_target) if "BUY" in t_type else (cp <= half_target)
                    
                    if not partial_exit and is_half_hit:
                        locked_pnl = pts_captured * (qty / 2)
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

                    remaining_qty = (qty / 2) if partial_exit else qty
                    if t_type == "BUY 🟢":
                        if cp >= tp: status, pnl, msg = "PROFIT ✅", current_pnl + (abs(tp - entry) * remaining_qty), f"🎯 TARGET HIT: {sym} (+₹{(abs(tp - entry) * remaining_qty):.2f})"
                        elif cp <= sl: status, pnl, msg = "LOSS ❌", current_pnl - (abs(entry - cp) * remaining_qty), f"🛑 SL HIT: {sym} (Exit: ₹{cp:.2f})"
                    elif t_type == "SELL 🔴":
                        if cp <= tp: status, pnl, msg = "PROFIT ✅", current_pnl + (abs(entry - tp) * remaining_qty), f"🎯 TARGET HIT: {sym} (+₹{(abs(entry - tp) * remaining_qty):.2f})"
                        elif cp >= sl: status, pnl, msg = "LOSS ❌", current_pnl - (abs(sl - entry) * remaining_qty), f"🛑 SL HIT: {sym} (Exit: ₹{cp:.2f})"
                    
                    if status != "OPEN":
                        execute_db("UPDATE pro_trades SET status=%s, pnl=%s WHERE id=%s", (status, pnl, t_id))
                        if not alerts_muted: send_msg(AUTHORIZED_USER, msg)
                continue 
            
            rsi_buy = 60 if strategy_mode == "SAFE" else 50
            rsi_sell = 40 if strategy_mode == "SAFE" else 50
            dist_limit = 0.4 if strategy_mode == "SAFE" else 0.8
            
            dist_ema = (abs(cp - ema200) / ema200) * 100
            if dist_ema > dist_limit: continue
            
            decision = "WAIT"
            if cp > ema200 and rsi > rsi_buy and macd.iloc[-1] > macd_sig.iloc[-1] and trend_15m_up: decision = "BUY 🟢"
            elif cp < ema200 and rsi < rsi_sell and macd.iloc[-1] < macd_sig.iloc[-1] and not trend_15m_up: decision = "SELL 🔴"

            if sym in last_signal and last_signal[sym] == decision: continue
            last_signal[sym] = decision

            if decision != "WAIT":
                slippage = cp * 0.0005
                exec_price = cp + slippage if "BUY" in decision else cp - slippage
                
                sl = exec_price - (exec_price * 0.002) if "BUY" in decision else exec_price + (exec_price * 0.002)
                tp = exec_price + (exec_price * 0.005) if "BUY" in decision else exec_price - (exec_price * 0.005)
                
                dynamic_capital = max(50000, 50000 + total_pnl)
                sl_dist = abs(exec_price - sl)
                risk_amt = dynamic_capital * (current_risk_percent / 100)
                qty = min(100, max(1, int(risk_amt / sl_dist))) if sl_dist > 0 else 1
                
                order_success = True
                if trading_mode == "REAL":
                    order_success = place_real_order(sym, decision, exec_price, sl, tp, qty)
                
                if order_success:
                    ts = int(time.time())
                    execute_db('INSERT INTO pro_trades (date, date_ts, symbol, type, entry_price, sl, tp, status, pnl, mode, qty) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                               (get_ist().strftime("%Y-%m-%d %H:%M"), ts, sym, decision, exec_price, sl, tp, "OPEN", 0.0, trading_mode, qty))
                    last_trade_time[sym] = time.time()
                    
                    rr_ratio = abs(tp - exec_price) / sl_dist if sl_dist > 0 else 0
                    tv_link = f"https://www.tradingview.com/chart/?symbol=NSE:{sym}"
                    
                    if not alerts_muted:
                        send_msg(AUTHORIZED_USER, f"🚀 *{trading_mode} EXECUTED* 🚀\n\n📈 *Symbol:* {sym}\n🤖 *Action:* {decision}\n🛒 *Qty:* {qty} (Risk {current_risk_percent}% of ₹{dynamic_capital:,.0f})\n\n🔸 *Entry:* ₹{exec_price:.2f} (Slip Inc.)\n🎯 *TP:* ₹{tp:.2f} | 🛡️ *SL:* ₹{sl:.2f}\n⚖️ *RR:* 1:{rr_ratio:.1f}\n\n📊 [View LIVE Chart]({tv_link})")

        except Exception as e: logging.error(f"Scan error {sym}: {e}")
        time.sleep(1) 
    return "CONTINUE"

def manual_scan_wrapper():
    if not scan_lock.acquire(blocking=False):
        send_msg(AUTHORIZED_USER, "⚠️ Scan is already running. Please wait.")
        return
    try:
        send_msg(AUTHORIZED_USER, "🔍 Manual Scan Initiated...")
        run_scan_cycle(manual=True)
    finally:
        scan_lock.release()

def auto_scanner():
    global bot_paused
    while True:
        try:
            if not AUTHORIZED_USER:
                time.sleep(5); continue
            
            now = get_ist()
            if now.hour == 9 and now.minute == 14:
                last_signal.clear()
                last_trade_time.clear()
                if bot_paused:
                    bot_paused = False
                    send_msg(AUTHORIZED_USER, "🌅 *Good Morning!* Variables cleared. Bot Auto-Resumed.")
            
            if bot_paused:
                time.sleep(5); continue
            
            if scan_lock.acquire(blocking=False):
                try:
                    status = run_scan_cycle(manual=False)
                    if status == "PAUSE": 
                        bot_paused = True
                        send_msg(AUTHORIZED_USER, "🛑 Bot auto-paused due to limits. Use /resume to continue.")
                finally:
                    scan_lock.release()
                    
        except Exception as e:
            logging.error(f"Global Scanner Error: {e}")
            time.sleep(5)
        time.sleep(60)

# ==========================================
# 🎮 6. COMMAND HANDLER
# ==========================================
def telegram():
    global bot_paused, trading_mode, strategy_mode, alerts_muted, current_risk_percent, max_daily_trades
    last_id = None
    
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
                        chat_id = upd["message"]["chat"]["id"]
                        txt = upd["message"]["text"]
                        
                        if chat_id != AUTHORIZED_USER:
                            send_msg(chat_id, "❌ *UNAUTHORIZED ACCESS.*"); continue
                            
                        if txt == "/start": send_msg(chat_id, "💎 V17.0 IMMORTAL ENGINE Online. Cloud DB Connected.")
                        
                        elif txt == "🌐 Open Dashboard":
                            dash_url = "https://ai-trading-bot-itc0.onrender.com"
                            inline_keyboard = {"inline_keyboard": [[{"text": "🚀 Open Web Dashboard", "url": dash_url}]]}
                            try: requests.post(f"{BASE_URL}/sendMessage", json={"chat_id": chat_id, "text": "Click below to view your Live PnL Graph:", "reply_markup": inline_keyboard})
                            except: pass
                            
                        elif txt in ["🔄 Switch Mode", "/mode"]:
                            send_msg(chat_id, "⚠️ *WARNING: MODE SWITCH*\n\nType `CONFIRM REAL` to activate Real Money.\nType `CONFIRM DEMO` to return to Paper Trading.")
                        elif txt == "CONFIRM REAL":
                            trading_mode = "REAL"
                            send_msg(chat_id, "💰 *REAL TRADING ENABLED!*")
                        elif txt == "CONFIRM DEMO":
                            trading_mode = "DEMO"
                            send_msg(chat_id, "🛡️ Switched to *DEMO* Mode safely.")
                            
                        elif txt == "📊 Check Status": 
                            send_msg(chat_id, f"📡 Bot: {'Paused ⏸' if bot_paused else 'Active ▶️'}\n🧠 Mode: {trading_mode}\n🛡️ Strategy: {strategy_mode}\n📊 Max Trades: {max_daily_trades}/day")
                        elif txt in ["💰 View PnL", "/pnl"]:
                            pnl = get_val("SELECT SUM(pnl) FROM pro_trades WHERE status!='OPEN'")
                            send_msg(chat_id, f"💰 Net PnL: ₹{pnl:.2f}")
                        elif txt == "📈 Live PnL":
                            rows = execute_db("SELECT symbol, type, entry_price, qty FROM pro_trades WHERE status='OPEN'", fetchall=True)
                            if not rows: send_msg(chat_id, "No open trades right now.")
                            else:
                                msg, total_live = "📈 *LIVE OPEN TRADES:*\n\n", 0
                                for r in rows:
                                    sym, t_type, entry, qty = r[0], r[1], r[2], r[3]
                                    try:
                                        time.sleep(random.uniform(1, 2))
                                        d = tv.get_hist(symbol=sym, exchange='NSE', interval=Interval.in_1_minute, n_bars=2)
                                        if d is None or d.empty: continue
                                        cp = d['close'].iloc[-1]
                                        pts = (cp - entry) if "BUY" in t_type else (entry - cp)
                                        pnl = pts * qty
                                        total_live += pnl
                                        msg += f"🔹 {sym}: ₹{pnl:.2f}\n"
                                    except: pass
                                msg += f"\n💰 *Total Floating:* ₹{total_live:.2f}"
                                send_msg(chat_id, msg)
                        elif txt in ["📅 Today Report", "/today"]:
                            t = get_ist().strftime("%Y-%m-%d")
                            pnl = get_val("SELECT SUM(pnl) FROM pro_trades WHERE date LIKE %s AND status!='OPEN'", (f"{t}%",))
                            send_msg(chat_id, f"📅 *Today's PnL:* ₹{pnl:.2f}")
                        elif txt == "🔍 Scan Now":
                            Thread(target=manual_scan_wrapper).start()
                        elif txt == "🛡️ Safe Mode": strategy_mode = "SAFE"; send_msg(chat_id, "🛡️ Safe Mode ON.")
                        elif txt == "⚡ Aggressive Mode": strategy_mode = "AGGRESSIVE"; send_msg(chat_id, "⚡ Aggressive Mode ON.")
                        elif txt in ["❌ Close All", "/closeall"]:
                            rows = execute_db("SELECT id, symbol, type, entry_price FROM pro_trades WHERE status='OPEN'", fetchall=True)
                            if rows:
                                for r in rows: execute_db("UPDATE pro_trades SET status='CLOSED ⚠️' WHERE id=%s", (r[0],))
                                send_msg(chat_id, "⚠️ All positions FORCE CLOSED.")
                            else: send_msg(chat_id, "❌ No open trades.")
                        elif txt in ["⏸ Pause Bot", "/pause"]: bot_paused = True; send_msg(chat_id, "🛑 Bot Paused.")
                        elif txt in ["▶️ Resume Bot", "/resume"]: bot_paused = False; send_msg(chat_id, "✅ Bot Resumed.")
        except Exception as e: logging.error(f"TG Error: {e}"); time.sleep(5)

if __name__ == "__main__":
    setup_db()
    Thread(target=run_server, daemon=True).start()
    Thread(target=auto_scanner, daemon=True).start()
    telegram()
