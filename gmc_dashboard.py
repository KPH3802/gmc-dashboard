#!/usr/bin/env python3
"""
GMC Portfolio Dashboard — Flask Backend
Auto-refreshing trading dashboard for Grist Mill Capital.
"""

import json
import logging
import os
import sqlite3
import threading
import time
from datetime import date, datetime, timedelta
from functools import lru_cache

import requests
import urllib3
import yfinance as yf
from flask import Flask, jsonify, render_template, send_from_directory

import config

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cache layer — thread-safe timed cache
# ---------------------------------------------------------------------------
_cache = {}
_cache_lock = threading.Lock()


def cached(key, ttl_seconds, fetcher):
    """Return cached value or call fetcher. Thread-safe."""
    with _cache_lock:
        entry = _cache.get(key)
        if entry and time.time() - entry["ts"] < ttl_seconds:
            return entry["data"]
    try:
        data = fetcher()
    except Exception as e:
        log.warning(f"Cache fetch failed for {key}: {e}")
        with _cache_lock:
            entry = _cache.get(key)
            if entry:
                entry["data"]["stale"] = True
                return entry["data"]
        return {"error": str(e), "stale": True}
    with _cache_lock:
        _cache[key] = {"data": data, "ts": time.time()}
    return data


# ---------------------------------------------------------------------------
# Price fetching — 3-source fallback (same as ib_autotrader.py)
# ---------------------------------------------------------------------------

def fetch_price(ticker, timeout=10):
    """yfinance -> Yahoo direct -> FMP. Returns float or None."""
    import pandas
    result = [None]

    def _yf():
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period="1d")
            if not hist.empty:
                if isinstance(hist.columns, pandas.MultiIndex):
                    hist.columns = [c[0] for c in hist.columns]
                result[0] = float(hist["Close"].iloc[-1])
        except Exception:
            pass

    t = threading.Thread(target=_yf, daemon=True)
    t.start()
    t.join(timeout=timeout)
    if result[0] is not None:
        return result[0]

    try:
        encoded = requests.utils.quote(ticker)
        r = requests.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded}",
            headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
        return float(r.json()["chart"]["result"][0]["meta"]["regularMarketPrice"])
    except Exception:
        pass

    try:
        r = requests.get(
            f"https://financialmodelingprep.com/api/v3/quote/{ticker}?apikey={config.FMP_API_KEY}",
            timeout=8)
        data = r.json()
        if data and isinstance(data, list) and "price" in data[0]:
            return float(data[0]["price"])
    except Exception:
        pass
    return None


def batch_fetch_prices(tickers, timeout=20):
    """Batch yfinance download, fallback per-ticker."""
    import pandas
    prices = {}
    if not tickers:
        return prices
    unique = list(set(tickers))

    result = [None]
    def _batch():
        try:
            df = yf.download(unique, period="5d", progress=False, threads=True)
            if df is not None and not df.empty:
                result[0] = df
        except Exception:
            pass

    t = threading.Thread(target=_batch, daemon=True)
    t.start()
    t.join(timeout=timeout)

    df = result[0]
    if df is not None:
        try:
            if isinstance(df.columns, pandas.MultiIndex):
                close_cols = df["Close"] if "Close" in df.columns.get_level_values(0) else None
                if close_cols is not None:
                    for tk in unique:
                        try:
                            col = close_cols[tk] if tk in close_cols.columns else None
                            if col is not None and not col.dropna().empty:
                                prices[tk] = float(col.dropna().iloc[-1])
                        except Exception:
                            pass
            else:
                if isinstance(df.columns, pandas.MultiIndex):
                    df.columns = [c[0] for c in df.columns]
                if "Close" in df.columns and not df["Close"].dropna().empty:
                    prices[unique[0]] = float(df["Close"].dropna().iloc[-1])
        except Exception:
            pass

    for tk in unique:
        if tk not in prices or prices[tk] is None:
            p = fetch_price(tk)
            if p is not None:
                prices[tk] = p
    return prices


# ---------------------------------------------------------------------------
# Database helpers (read-only)
# ---------------------------------------------------------------------------

def read_positions(status_filter=None):
    """Read positions from positions.db."""
    if not os.path.exists(config.POSITIONS_DB):
        return []
    conn = sqlite3.connect(config.POSITIONS_DB)
    conn.row_factory = sqlite3.Row
    if status_filter:
        rows = conn.execute("SELECT * FROM open_positions WHERE status=?", (status_filter,)).fetchall()
    else:
        rows = conn.execute("SELECT * FROM open_positions").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def read_benchmarks():
    """Read signal_benchmarks table."""
    bm = {}
    if not os.path.exists(config.POSITIONS_DB):
        return bm
    try:
        conn = sqlite3.connect(config.POSITIONS_DB)
        for row in conn.execute("SELECT source, direction, expected_return_pct, expected_hold_days FROM signal_benchmarks"):
            bm[(row[0], row[1])] = {"expected_return_pct": row[2], "expected_hold_days": row[3]}
        conn.close()
    except Exception:
        pass
    return bm


# ---------------------------------------------------------------------------
# Coinbase (same pattern as performance_tracker.py)
# ---------------------------------------------------------------------------

def get_coinbase_value():
    """Returns (total_usd, positions_list) or (None, [])."""
    try:
        from coinbase.rest import RESTClient
        with open(config.COINBASE_CDP_KEY) as f:
            creds = json.load(f)
        client = RESTClient(api_key=creds["name"], api_secret=creds["privateKey"])
        portfolios = client.get_portfolios()
        port_list = portfolios.get("portfolios", []) if isinstance(portfolios, dict) else getattr(portfolios, "portfolios", [])
        if not port_list:
            return None, []
        uuid = port_list[0].get("uuid", "") if isinstance(port_list[0], dict) else getattr(port_list[0], "uuid", "")
        breakdown = client.get(f"/api/v3/brokerage/portfolios/{uuid}")
        balances = breakdown.get("breakdown", {}).get("portfolio_balances", {})
        cash = float(balances.get("total_cash_equivalent_balance", {}).get("value", 0))
        crypto = float(balances.get("total_crypto_balance", {}).get("value", 0))
        total = round(cash + crypto, 2)
        positions = []
        for pos in breakdown.get("breakdown", {}).get("spot_positions", []):
            fiat_val = float(pos.get("total_balance_fiat", 0))
            if fiat_val > 0.01 or pos.get("is_cash"):
                positions.append({"currency": pos.get("asset", ""), "usd_value": round(fiat_val, 2)})
        return total, positions
    except Exception as e:
        log.warning(f"Coinbase failed: {e}")
        return None, []


# ---------------------------------------------------------------------------
# IB Gateway helper
# ---------------------------------------------------------------------------

def get_ib_account_value(account_id):
    """Get account value from IB Gateway. Returns float or None."""
    try:
        r = requests.get(
            f"{config.IB_GATEWAY_URL}/portfolio/{account_id}/summary",
            verify=False, timeout=5)
        if r.status_code == 200:
            data = r.json()
            for item in data:
                if item.get("id") == "netliquidation":
                    return float(item.get("amount", 0))
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# API: /api/positions
# ---------------------------------------------------------------------------

def _fetch_positions():
    today = date.today()
    open_pos = read_positions("OPEN")
    benchmarks = read_benchmarks()

    tickers = [p["ticker"] for p in open_pos]
    prices = batch_fetch_prices(tickers) if tickers else {}

    result = []
    for p in open_pos:
        tk = p["ticker"]
        direction = p["direction"] or "BUY"
        entry_price = p["entry_price"]
        current = prices.get(tk)

        try:
            entry_dt = datetime.strptime(p["entry_date"], "%Y-%m-%d").date()
            days_held = (today - entry_dt).days
        except Exception:
            days_held = None

        # Return calculation
        return_pct = None
        if current and entry_price and entry_price != 0:
            if direction == "SHORT":
                return_pct = round(((entry_price - current) / entry_price) * 100, 2)
            else:
                return_pct = round(((current - entry_price) / entry_price) * 100, 2)

        # Expected values from DB or benchmarks
        exp_ret = p.get("expected_return_pct")
        exp_hold = p.get("expected_hold_days")
        source = p.get("source") or "UNKNOWN"
        if exp_ret is None or exp_hold is None:
            bm = benchmarks.get((source, direction), {})
            if exp_ret is None:
                exp_ret = bm.get("expected_return_pct")
            if exp_hold is None:
                exp_hold = bm.get("expected_hold_days")

        prorated = None
        vs_pace = None
        status_label = "N/A"
        days_remaining = None
        if days_held is not None and exp_hold and exp_ret is not None and exp_hold > 0:
            prorated = round((days_held / exp_hold) * exp_ret, 2)
            if return_pct is not None:
                vs_pace = round(return_pct - prorated, 2)
                if vs_pace > 0.5:
                    status_label = "AHEAD"
                elif vs_pace < -0.5:
                    status_label = "BEHIND"
                else:
                    status_label = "ON PACE"
            days_remaining = max(0, exp_hold - days_held)

        result.append({
            "ticker": tk, "source": source, "direction": direction,
            "entry_date": p["entry_date"], "entry_price": round(entry_price, 2),
            "shares": p["shares"], "position_size": p["position_size"],
            "current_price": round(current, 2) if current else None,
            "return_pct": return_pct,
            "expected_return_pct": exp_ret, "expected_hold_days": exp_hold,
            "days_held": days_held, "days_remaining": days_remaining,
            "prorated_expected": prorated, "vs_pace": vs_pace,
            "status": status_label,
            "stale": current is None,
        })

    result.sort(key=lambda x: x.get("days_remaining") or 9999)
    return {"positions": result, "count": len(result), "stale": False}


# ---------------------------------------------------------------------------
# API: /api/portfolio
# ---------------------------------------------------------------------------

def _fetch_portfolio():
    today = date.today()

    # --- Bedrock ---
    bedrock_tickers = [h["ticker"] for h in config.BEDROCK_HOLDINGS]
    prices = batch_fetch_prices(bedrock_tickers + ["SPY"])
    bedrock_current = 0.0
    bedrock_holdings = []
    for h in config.BEDROCK_HOLDINGS:
        p = prices.get(h["ticker"])
        cost = h["shares"] * h["cost_per_share"]
        val = h["shares"] * p if p else None
        if val:
            bedrock_current += val
        bedrock_holdings.append({
            "ticker": h["ticker"], "shares": h["shares"],
            "cost": round(cost, 2), "current": round(val, 2) if val else None,
            "return_pct": round(((p - h["cost_per_share"]) / h["cost_per_share"]) * 100, 2) if p else None,
        })
    bedrock_ret = round(((bedrock_current - config.BEDROCK_TOTAL_COST) / config.BEDROCK_TOTAL_COST) * 100, 2) if bedrock_current else None

    # SPY benchmark from bedrock entry
    spy_ret = None
    try:
        import pandas
        df = yf.download("SPY", start=config.BEDROCK_ENTRY_DATE,
                         end=(today + timedelta(days=1)).isoformat(), progress=False)
        if df is not None and not df.empty:
            if isinstance(df.columns, pandas.MultiIndex):
                df.columns = [c[0] for c in df.columns]
            spy_start = float(df["Close"].iloc[0])
            spy_end = float(df["Close"].iloc[-1])
            spy_ret = round(((spy_end - spy_start) / spy_start) * 100, 2)
    except Exception:
        pass

    entry_dt = datetime.strptime(config.BEDROCK_ENTRY_DATE, "%Y-%m-%d").date()
    bedrock = {
        "name": "Bedrock", "cost_basis": config.BEDROCK_TOTAL_COST,
        "current_value": round(bedrock_current, 2) if bedrock_current else None,
        "return_pct": bedrock_ret, "days_held": (today - entry_dt).days,
        "spy_return_pct": spy_ret, "holdings": bedrock_holdings, "stale": bedrock_current == 0,
    }

    # --- Event Alpha ---
    open_pos = read_positions("OPEN")
    deployed = sum(p["position_size"] for p in open_pos if p.get("position_size"))
    closed = read_positions("CLOSED")
    total_pnl = sum((p.get("return_pct") or 0) / 100 * (p.get("position_size") or 0) for p in closed)
    ib_val = get_ib_account_value(config.IB_ACCOUNT_PAPER)
    ea_acct = ib_val or config.EVENT_ALPHA_FALLBACK
    ea_ret = round((total_pnl / ea_acct) * 100, 2) if ea_acct else None

    event_alpha = {
        "name": "Event Alpha", "account_value": ea_acct,
        "deployed_capital": round(deployed, 2), "open_positions": len(open_pos),
        "closed_trades": len(closed), "total_pnl": round(total_pnl, 2),
        "return_pct": ea_ret, "stale": ib_val is None,
    }

    # --- Digital Alpha ---
    cb_val, cb_pos = get_coinbase_value()
    da_baseline = config.DIGITAL_ALPHA_BASELINE
    da_ret = round(((cb_val - da_baseline) / da_baseline) * 100, 2) if cb_val else None
    btc_ret = None
    try:
        import pandas
        df = yf.download("BTC-USD", start=config.DIGITAL_ALPHA_BASELINE_DATE,
                         end=(today + timedelta(days=1)).isoformat(), progress=False)
        if df is not None and not df.empty:
            if isinstance(df.columns, pandas.MultiIndex):
                df.columns = [c[0] for c in df.columns]
            btc_s = float(df["Close"].iloc[0])
            btc_e = float(df["Close"].iloc[-1])
            btc_ret = round(((btc_e - btc_s) / btc_s) * 100, 2)
    except Exception:
        pass

    digital_alpha = {
        "name": "Digital Alpha", "current_value": cb_val,
        "baseline": da_baseline, "return_pct": da_ret,
        "btc_hold_pct": btc_ret, "positions": cb_pos, "stale": cb_val is None,
    }

    return {"bedrock": bedrock, "event_alpha": event_alpha, "digital_alpha": digital_alpha, "stale": False}


# ---------------------------------------------------------------------------
# API: /api/market
# ---------------------------------------------------------------------------

def _fetch_market():
    tickers = {
        "equities": ["SPY", "^DJI", "QQQ", "^VIX"],
        "crypto": ["BTC-USD", "ETH-USD", "SOL-USD"],
    }
    all_tks = tickers["equities"] + tickers["crypto"]
    prices = batch_fetch_prices(all_tks)

    def _build(tk, label=None):
        p = prices.get(tk)
        return {"symbol": label or tk, "price": round(p, 2) if p else None, "stale": p is None}

    return {
        "equities": [
            _build("SPY", "SPY"), _build("^DJI", "DJI"),
            _build("QQQ", "QQQ"), _build("^VIX", "VIX"),
        ],
        "crypto": [
            _build("BTC-USD", "BTC"), _build("ETH-USD", "ETH"), _build("SOL-USD", "SOL"),
        ],
        "stale": False,
    }


# ---------------------------------------------------------------------------
# API: /api/sentiment
# ---------------------------------------------------------------------------

def _fetch_sentiment():
    try:
        r = requests.get("https://api.alternative.me/fng/?limit=2", timeout=10)
        data = r.json().get("data", [])
        if data:
            current = data[0]
            prev = data[1] if len(data) > 1 else {}
            return {
                "value": int(current.get("value", 50)),
                "classification": current.get("value_classification", "Neutral"),
                "previous_value": int(prev.get("value", 50)) if prev else None,
                "stale": False,
            }
    except Exception as e:
        log.warning(f"Sentiment fetch failed: {e}")
    return {"value": None, "classification": "Unknown", "stale": True}


# ---------------------------------------------------------------------------
# API: /api/equity_curves
# ---------------------------------------------------------------------------

def _fetch_equity_curves():
    import pandas
    today = date.today()
    end_str = (today + timedelta(days=1)).isoformat()

    # --- Bedrock curve (indexed to 100) ---
    bedrock_curve = []
    spy_curve = []
    try:
        tks = [h["ticker"] for h in config.BEDROCK_HOLDINGS] + ["SPY"]
        df = yf.download(tks, start=config.BEDROCK_ENTRY_DATE, end=end_str, progress=False)
        if df is not None and not df.empty:
            close = df["Close"] if "Close" in df.columns.get_level_values(0) else df
            # Equal-weight portfolio
            first_day = {}
            for h in config.BEDROCK_HOLDINGS:
                col = close[h["ticker"]] if h["ticker"] in close.columns else None
                if col is not None and not col.dropna().empty:
                    first_day[h["ticker"]] = float(col.dropna().iloc[0])
            if first_day:
                for i, idx in enumerate(close.index):
                    dt_str = str(idx.date())
                    port_val = 0
                    base_val = 0
                    for h in config.BEDROCK_HOLDINGS:
                        if h["ticker"] in first_day:
                            try:
                                p = float(close[h["ticker"]].iloc[i])
                                port_val += h["shares"] * p
                                base_val += h["shares"] * first_day[h["ticker"]]
                            except Exception:
                                pass
                    if base_val > 0:
                        bedrock_curve.append({"date": dt_str, "value": round(100 * port_val / base_val, 2)})
            # SPY curve
            spy_col = close["SPY"] if "SPY" in close.columns else None
            if spy_col is not None and not spy_col.dropna().empty:
                spy_first = float(spy_col.dropna().iloc[0])
                for i, idx in enumerate(spy_col.index):
                    try:
                        v = float(spy_col.iloc[i])
                        spy_curve.append({"date": str(idx.date()), "value": round(100 * v / spy_first, 2)})
                    except Exception:
                        pass
    except Exception as e:
        log.warning(f"Bedrock curve failed: {e}")

    # --- Event Alpha (cumulative P&L from closed trades) ---
    ea_curve = []
    closed = read_positions("CLOSED")
    if closed:
        trades = []
        for p in closed:
            if p.get("close_date") and p.get("return_pct") is not None:
                pnl = (p["return_pct"] / 100) * (p.get("position_size") or 0)
                trades.append({"date": p["close_date"], "pnl": pnl})
        trades.sort(key=lambda x: x["date"])
        cum = 0
        for t in trades:
            cum += t["pnl"]
            ea_curve.append({"date": t["date"], "value": round(cum, 2)})

    # --- Digital Alpha (BTC proxy shape) ---
    da_curve = []
    try:
        df = yf.download("BTC-USD", start=config.DIGITAL_ALPHA_BASELINE_DATE, end=end_str, progress=False)
        if df is not None and not df.empty:
            if isinstance(df.columns, pandas.MultiIndex):
                df.columns = [c[0] for c in df.columns]
            first = float(df["Close"].dropna().iloc[0])
            baseline = config.DIGITAL_ALPHA_BASELINE
            for i, idx in enumerate(df.index):
                try:
                    btc_p = float(df["Close"].iloc[i])
                    da_curve.append({
                        "date": str(idx.date()),
                        "value": round(baseline * btc_p / first, 2),
                        "estimated": True,
                    })
                except Exception:
                    pass
    except Exception:
        pass

    return {
        "bedrock": {"portfolio": bedrock_curve, "spy": spy_curve},
        "event_alpha": ea_curve,
        "digital_alpha": da_curve,
        "stale": False,
    }


# ---------------------------------------------------------------------------
# API: /api/scorecard
# ---------------------------------------------------------------------------

def _fetch_scorecard():
    closed = read_positions("CLOSED")
    if not closed:
        return {"signals": {}, "total": {"status": "no_data"}, "stale": False}

    by_source = {}
    for p in closed:
        src = p.get("source") or "UNKNOWN"
        if src not in by_source:
            by_source[src] = []
        by_source[src].append(p)

    signals = {}
    total_trades = 0
    total_wins = 0
    total_ret = 0.0

    for src, trades in by_source.items():
        returns = [t["return_pct"] for t in trades if t.get("return_pct") is not None]
        n = len(returns)
        if n == 0:
            signals[src] = {"trades": len(trades), "status": "no_data"}
            continue
        wins = sum(1 for r in returns if r > 0)
        avg_ret = round(sum(returns) / n, 2)
        vs_exp = [t.get("vs_expected_pct") for t in trades if t.get("vs_expected_pct") is not None]
        avg_vs = round(sum(vs_exp) / len(vs_exp), 2) if vs_exp else None
        alphas = [t.get("alpha_vs_spy") for t in trades if t.get("alpha_vs_spy") is not None]
        avg_alpha = round(sum(alphas) / len(alphas), 2) if alphas else None

        signals[src] = {
            "trades": n, "win_rate": round(100 * wins / n, 1),
            "avg_return": avg_ret, "avg_vs_expected": avg_vs,
            "avg_alpha": avg_alpha,
        }
        total_trades += n
        total_wins += wins
        total_ret += sum(returns)

    total = {
        "trades": total_trades,
        "win_rate": round(100 * total_wins / total_trades, 1) if total_trades else None,
        "avg_return": round(total_ret / total_trades, 2) if total_trades else None,
    }

    return {"signals": signals, "total": total, "stale": False}


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("dashboard.html")


@app.route("/static/<path:filename>")
def static_files(filename):
    return send_from_directory("static", filename)


@app.route("/api/positions")
def api_positions():
    return jsonify(cached("positions", 60, _fetch_positions))


@app.route("/api/portfolio")
def api_portfolio():
    return jsonify(cached("portfolio", 60, _fetch_portfolio))


@app.route("/api/market")
def api_market():
    return jsonify(cached("market", 60, _fetch_market))


@app.route("/api/sentiment")
def api_sentiment():
    return jsonify(cached("sentiment", 300, _fetch_sentiment))


@app.route("/api/equity_curves")
def api_equity_curves():
    return jsonify(cached("equity_curves", 300, _fetch_equity_curves))


@app.route("/api/scorecard")
def api_scorecard():
    return jsonify(cached("scorecard", 300, _fetch_scorecard))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    log.info(f"GMC Dashboard starting on {config.DASHBOARD_HOST}:{config.DASHBOARD_PORT}")
    app.run(host=config.DASHBOARD_HOST, port=config.DASHBOARD_PORT, debug=False)
