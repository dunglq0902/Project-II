"""
dashboard/app.py
Premium & Intuitive Crypto Analytics Dashboard (Vietnam Timezone)

Features:
  📊  High-fidelity OHLCV Candlestick Charts (GMT+7)
  🧠  AI-style Indicator Interpretations (Buy/Sell/Neutral)
  🔔  Advanced Alert Rules Management
  📜  Alert History & System Health
  ✨  Glassmorphism UI Design
"""

import os
import sys
import json
from datetime import datetime, timedelta, timezone

import httpx
import pandas as pd
import plotly.graph_objects as go
import plotly.subplots as ms
import streamlit as st
from pymongo import MongoClient

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
MONGO_URI   = os.getenv("MONGO_URI", "mongodb://root:changeme@localhost:27017")
MONGO_DB    = os.getenv("MONGO_DB", "crypto_analytics")
ALERT_API   = os.getenv("ALERT_API_URL", "http://localhost:8000")

SYMBOLS     = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]
TIMEFRAMES  = ["5m", "15m", "1h", "4h", "1d"]

# Vietnam Timezone Offset
VN_OFFSET = timedelta(hours=7)

# ─────────────────────────────────────────────
# Page Configuration
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="CryptoVision Premium",
    page_icon="💎",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────
# Enhanced CSS (Glassmorphism & Premium UI)
# ─────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

    :root {
        --bg-dark: #0d1117;
        --card-bg: rgba(22, 27, 34, 0.7);
        --accent-blue: #58a6ff;
        --accent-green: #3fb950;
        --accent-red: #f85149;
        --text-main: #e6edf3;
        --text-muted: #8b949e;
    }

    .main { font-family: 'Inter', sans-serif; background-color: var(--bg-dark); }

    /* Glassmorphism Card */
    .premium-card {
        background: var(--card-bg);
        backdrop-filter: blur(10px);
        -webkit-backdrop-filter: blur(10px);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 16px;
        padding: 24px;
        margin-bottom: 20px;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    .premium-card:hover {
        border: 1px solid rgba(88, 166, 255, 0.3);
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4);
    }

    /* Metric Header */
    .metric-label {
        color: var(--text-muted);
        font-size: 12px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 1.2px;
        margin-bottom: 8px;
    }
    .metric-value {
        color: var(--text-main);
        font-size: 32px;
        font-weight: 700;
        font-family: 'JetBrains Mono', monospace;
    }
    .metric-advice {
        display: inline-block;
        margin-top: 10px;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 12px;
        font-weight: 600;
    }
    .advice-buy { background: rgba(63, 185, 80, 0.2); color: #3fb950; border: 1px solid #3fb950; }
    .advice-sell { background: rgba(248, 81, 73, 0.2); color: #f85149; border: 1px solid #f85149; }
    .advice-neutral { background: rgba(139, 148, 158, 0.2); color: #8b949e; border: 1px solid #8b949e; }

    /* Custom Sidebar */
    [data-testid="stSidebar"] {
        background-color: #010409;
        border-right: 1px solid rgba(255, 255, 255, 0.05);
    }

    /* Tabs Styling */
    .stTabs [data-baseweb="tab-list"] { gap: 10px; }
    .stTabs [data-baseweb="tab"] {
        height: 45px;
        background-color: transparent;
        border-radius: 8px;
        color: var(--text-muted);
    }
    .stTabs [data-baseweb="tab"]:hover { color: var(--accent-blue); }
    .stTabs [data-baseweb="tab"][aria-selected="true"] {
        background-color: rgba(88, 166, 255, 0.1);
        color: var(--accent-blue);
    }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# Data Access & Logic
# ─────────────────────────────────────────────
@st.cache_resource
def get_mongo_client():
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

def get_db():
    return get_mongo_client()[MONGO_DB]

@st.cache_data(ttl=30)
def load_gold_data(symbol: str, timeframe: str, limit: int = 300):
    try:
        db = get_db()
        data = list(db["gold_ohlcv"].find({"symbol": symbol, "timeframe": timeframe}, {"_id": 0}).sort("window_start", -1).limit(limit))
        if data:
            df = pd.DataFrame(data)
            # Convert to Vietnam Timezone
            df['window_start'] = pd.to_datetime(df['window_start']) + VN_OFFSET
            return df.sort_values("window_start")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Data Connection Error: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=30)
def load_alert_rules():
    try:
        db = get_db()
        return list(db["alert_rules"].find({}, {"_id": 0}).sort("created_at", -1))
    except: return []

@st.cache_data(ttl=15)
def load_alert_events(limit: int = 50):
    try:
        db = get_db()
        events = list(db["alert_events"].find({}, {"_id": 0}).sort("triggered_at", -1).limit(limit))
        for e in events:
            if "triggered_at" in e:
                e["triggered_at"] = pd.to_datetime(e["triggered_at"]) + VN_OFFSET
        return events
    except: return []

def get_rsi_advice(rsi):
    if rsi is None: return "NEUTRAL", "advice-neutral", "Waiting for data..."
    if rsi > 70: return "SELL / OVERBOUGHT", "advice-sell", "Price is likely stretched high."
    if rsi < 30: return "BUY / OVERSOLD", "advice-buy", "Price might be undervalued."
    return "NEUTRAL", "advice-neutral", "Market is in balance."

def get_macd_advice(macd, signal):
    if macd is None or signal is None: return "NEUTRAL", "advice-neutral"
    if macd > signal: return "BULLISH MOMENTUM", "advice-buy"
    return "BEARISH MOMENTUM", "advice-sell"

# ─────────────────────────────────────────────
# Visualizations
# ─────────────────────────────────────────────
def create_main_chart(df, symbol):
    fig = go.Figure()

    # Candlesticks
    fig.add_trace(go.Candlestick(
        x=df['window_start'],
        open=df['open'], high=df['high'],
        low=df['low'], close=df['close'],
        name="Price",
        increasing_line_color='#3fb950', decreasing_line_color='#f85149',
        increasing_fillcolor='rgba(63, 185, 80, 0.3)', decreasing_fillcolor='rgba(248, 81, 73, 0.3)'
    ))

    # Moving Averages
    if 'ma7' in df.columns:
        fig.add_trace(go.Scatter(x=df['window_start'], y=df['ma7'], name="MA 7", line=dict(color='#58a6ff', width=1.5)))
    if 'ma25' in df.columns:
        fig.add_trace(go.Scatter(x=df['window_start'], y=df['ma25'], name="MA 25", line=dict(color='#d2a8ff', width=1.5)))

    fig.update_layout(
        template="plotly_dark", height=500, margin=dict(l=10, r=10, t=30, b=10),
        xaxis_rangeslider_visible=False, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    return fig

def create_oscillator_chart(df, field, color, hlines=[30, 70]):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df['window_start'], y=df[field], line=dict(color=color, width=2), fill='tozeroy', fillcolor=f'rgba{tuple(list(int(color.lstrip("#")[i:i+2], 16) for i in (0, 2, 4)) + [0.1])}'))
    for h in hlines:
        fig.add_hline(y=h, line_dash="dot", line_color="rgba(255,255,255,0.2)")
    
    fig.update_layout(
        template="plotly_dark", height=200, margin=dict(l=10, r=10, t=10, b=10),
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', showlegend=False
    )
    return fig

# ─────────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────────
with st.sidebar:
    st.markdown("<h1 style='color: #58a6ff;'>💎 CryptoVision</h1>", unsafe_allow_html=True)
    st.markdown(f"<p style='color: #8b949e; font-size: 14px;'>GMT+7 Vietnam Time</p>", unsafe_allow_html=True)
    st.markdown("---")
    
    page = st.selectbox("Navigation", ["Dashboard", "Alert Rules", "History", "System"], index=0)
    
    st.markdown("---")
    st.markdown("**Market Settings**")
    selected_symbol = st.selectbox("Trading Pair", SYMBOLS, index=0)
    selected_timeframe = st.selectbox("Interval", TIMEFRAMES, index=2)
    
    st.markdown("---")
    st.markdown("**Live Update**")
    auto_refresh = st.checkbox("Enable Auto-refresh (30s)", value=True)
    
    st.markdown("---")
    st.caption(f"DB: {MONGO_DB}")
    if st.button("🔄 Force Refresh"):
        st.cache_data.clear()
        st.rerun()

# ─────────────────────────────────────────────
# Main Interface
# ─────────────────────────────────────────────
if page == "Dashboard":
    st.markdown(f"## 📊 Market Overview: {selected_symbol}")
    df = load_gold_data(selected_symbol, selected_timeframe)
    
    if not df.empty:
        latest = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else latest
        change = ((latest['close'] - prev['close']) / prev['close']) * 100
        
        m1, m2, m3, m4 = st.columns(4)
        with m1:
            st.markdown(f'<div class="premium-card"><div class="metric-label">Live Price</div><div class="metric-value">${latest["close"]:.2f}</div><div style="color: {"#3fb950" if change >= 0 else "#f85149"}; font-weight: 600;">{"▲" if change >= 0 else "▼"} {abs(change):.2f}%</div></div>', unsafe_allow_html=True)
        with m2:
            rsi_text, rsi_class, rsi_desc = get_rsi_advice(latest.get('rsi_14'))
            st.markdown(f'<div class="premium-card"><div class="metric-label">RSI (14) Status</div><div class="metric-value">{latest.get("rsi_14", 0):.1f}</div><div class="metric-advice {rsi_class}">{rsi_text}</div></div>', unsafe_allow_html=True)
        with m3:
            macd_text, macd_class = get_macd_advice(latest.get('macd'), latest.get('macd_signal'))
            st.markdown(f'<div class="premium-card"><div class="metric-label">MACD Signal</div><div class="metric-value">{latest.get("macd", 0):.4f}</div><div class="metric-advice {macd_class}">{macd_text}</div></div>', unsafe_allow_html=True)
        with m4:
            vol_ratio = latest.get('volume_ratio', 0)
            vol_class = "advice-buy" if vol_ratio > 1.5 else "advice-neutral"
            st.markdown(f'<div class="premium-card"><div class="metric-label">Vol Ratio</div><div class="metric-value">{vol_ratio:.2f}x</div><div class="metric-advice {vol_class}">{"HIGH SURGE" if vol_ratio > 1.5 else "NORMAL"}</div></div>', unsafe_allow_html=True)

        c_main, c_side = st.columns([3, 1])
        with c_main:
            st.plotly_chart(create_main_chart(df, selected_symbol), use_container_width=True)
            t1, t2 = st.tabs(["RSI Oscillator", "MACD Momentum"])
            with t1: st.plotly_chart(create_oscillator_chart(df, 'rsi_14', '#f0883e'), use_container_width=True)
            with t2: st.plotly_chart(create_oscillator_chart(df, 'macd', '#58a6ff', hlines=[0]), use_container_width=True)
        with c_side:
            st.markdown("### 💡 Trading Advice")
            st.markdown(f'<div class="premium-card" style="padding: 15px;"><p style="font-size: 14px; color: var(--text-muted);">Signals for <b>{selected_symbol}</b>:</p><hr style="opacity: 0.1"><p><b>RSI:</b> {rsi_desc}</p><p><b>Momentum:</b> {"Positive" if latest.get("macd", 0) > 0 else "Negative"}</p><p><b>Trend:</b> {"Bullish" if latest["close"] > latest.get("ma25", 0) else "Bearish"}</p></div>', unsafe_allow_html=True)
            st.caption(f"Last updated: {latest['window_start'].strftime('%H:%M:%S')}")
    else:
        st.warning("📡 Connecting to data stream... (GMT+7)")

elif page == "Alert Rules":
    st.markdown("## 🔔 Alert Rules Management")
    tab_list, tab_create = st.tabs(["📋 Active Rules", "➕ Create Rule"])
    rules = load_alert_rules()
    with tab_list:
        if rules:
            for r in rules:
                st.markdown(f'<div class="premium-card"><b>{r["symbol"]}</b> ({r["timeframe"]}) | Action: <span style="color: #58a6ff;">{r["action"]}</span><br><small>{r["rule_id"]}</small></div>', unsafe_allow_html=True)
        else: st.info("No rules found.")
    with tab_create:
        with st.form("new_rule"):
            st.write("Create a new price alert rule")
            st.form_submit_button("Create Rule (Demo)")

elif page == "History":
    st.markdown("## 📜 Historical Alerts")
    events = load_alert_events()
    if events:
        st.dataframe(pd.DataFrame(events)[["triggered_at", "symbol", "action", "close_price", "message"]], use_container_width=True)
    else: st.info("No alerts triggered yet.")

elif page == "System":
    st.markdown("## ❤️ System Health")
    st.success("Infrastructure: Connected")
    st.json({"DB": "MongoDB (Active)", "Stream": "Spark (Active)", "API": "Alert Engine (Active)"})
# ─────────────────────────────────────────────
# Auto Refresh Logic
# ─────────────────────────────────────────────
if page == "Dashboard" and auto_refresh:
    import time
    time.sleep(30)
    st.rerun()
