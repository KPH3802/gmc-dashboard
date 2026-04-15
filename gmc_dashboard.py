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

    # Bedrock daily change (prev close → current)
    bedrock_day_change_pct = None
    bedrock_day_change_dollar = None
    try:
        bedrock_prev_value = 0.0
        all_ok = True
        for h in config.BEDROCK_HOLDINGS:
            pc = yf.Ticker(h["ticker"]).fast_info.get("previousClose")
            if pc is None:
                all_ok = False
                break
            bedrock_prev_value += h["shares"] * pc
        if all_ok and bedrock_prev_value > 0 and bedrock_current > 0:
            bedrock_day_change_dollar = round(bedrock_current - bedrock_prev_value, 2)
            bedrock_day_change_pct = round((bedrock_day_change_dollar / bedrock_prev_value) * 100, 2)
    except Exception:
        bedrock_day_change_pct = None
        bedrock_day_change_dollar = None

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
        "day_change_pct": bedrock_day_change_pct, "day_change_dollar": bedrock_day_change_dollar,
    }

    # --- Event Alpha ---
    open_pos = read_positions("OPEN")
    deployed = sum(p["position_size"] for p in open_pos if p.get("position_size"))
    closed = read_positions("CLOSED")
    total_pnl = sum((p.get("return_pct") or 0) / 100 * (p.get("position_size") or 0) for p in closed)
    ib_val = get_ib_account_value(config.IB_ACCOUNT_PAPER)
    cache_path = os.path.expanduser("~/gmc_data/last_ea_value.txt")
    if ib_val is not None:
        try:
            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
            with open(cache_path, "w") as f:
                f.write(str(ib_val))
        except Exception:
            pass
        ea_acct = ib_val
        ea_source = "live"
    else:
        cached_val = None
        try:
            if os.path.exists(cache_path):
                with open(cache_path, "r") as f:
                    cached_val = float(f.read().strip())
        except Exception:
            cached_val = None
        ea_acct = cached_val if cached_val else config.EVENT_ALPHA_FALLBACK
        ea_source = "cache" if cached_val else "fallback"
    ea_ret = round((total_pnl / ea_acct) * 100, 2) if ea_acct else None

    # Event Alpha daily P&L from IB Gateway
    ea_day_pnl = None
    try:
        r = requests.get(
            f"{config.IB_GATEWAY_URL}/portfolio/{config.IB_ACCOUNT_PAPER}/summary",
            verify=False, timeout=5)
        if r.status_code == 200:
            for item in r.json():
                if item.get("id") == "dailypnl":
                    ea_day_pnl = round(float(item.get("amount", 0)), 2)
                    break
    except Exception:
        pass

    event_alpha = {
        "name": "Event Alpha", "account_value": ea_acct,
        "deployed_capital": round(deployed, 2), "open_positions": len(open_pos),
        "closed_trades": len(closed), "total_pnl": round(total_pnl, 2),
        "return_pct": ea_ret, "stale": ib_val is None, "account_source": ea_source,
        "day_pnl": ea_day_pnl,
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

    # Digital Alpha daily change (value-weighted across crypto positions)
    da_day_change_pct = None
    try:
        if cb_pos and cb_val and cb_val > 0:
            weighted_sum = 0.0
            total_weight = 0.0
            for pos in cb_pos:
                currency = pos.get("currency", "")
                usd_val = pos.get("usd_value", 0)
                if not currency or usd_val <= 0.01 or currency == "USD":
                    continue
                chg = yf.Ticker(f"{currency}-USD").fast_info.get("regularMarketChangePercent")
                if chg is not None:
                    weighted_sum += float(chg) * usd_val
                    total_weight += usd_val
            if total_weight > 0:
                da_day_change_pct = round(weighted_sum / total_weight, 2)
    except Exception:
        da_day_change_pct = None

    digital_alpha = {
        "name": "Digital Alpha", "current_value": cb_val,
        "baseline": da_baseline, "return_pct": da_ret,
        "btc_hold_pct": btc_ret, "positions": cb_pos, "stale": cb_val is None,
        "day_change_pct": da_day_change_pct,
    }

    # Combined GMC total
    try:
        combined_cost = config.BEDROCK_TOTAL_COST + (config.DIGITAL_ALPHA_BASELINE or 0)
        combined_value = (bedrock_current or 0) + (cb_val or 0)
        combined_pnl = (combined_value - combined_cost) + (total_pnl or 0)
        combined_ret = round((combined_pnl / combined_cost) * 100, 2) if combined_cost else None
    except Exception:
        combined_pnl = None
        combined_ret = None
    combined = {
        "total_pnl": round(combined_pnl, 2) if combined_pnl is not None else None,
        "return_pct": combined_ret,
        "total_value": round(combined_value, 2) if combined_value else None,
    }
    return {"bedrock": bedrock, "event_alpha": event_alpha, "digital_alpha": digital_alpha, "combined": combined, "stale": False}


# ---------------------------------------------------------------------------
# API: /api/market
# ---------------------------------------------------------------------------

def _fetch_market():
    tickers = {
        "equities": ["SPY", "^DJI", "QQQ", "^VIX", "^OVX", "^GVZ"],
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
        "volatility": [_build("^OVX", "OVX"), _build("^GVZ", "GVZ")],
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
        "bedrock": {"portfolio": [p for p in bedrock_curve if p.get("value")==p.get("value")], "spy": [p for p in spy_curve if p.get("value")==p.get("value")]},
        "event_alpha": [p for p in ea_curve if p.get("value")==p.get("value")],
        "digital_alpha": [p for p in da_curve if p.get('value') == p.get('value')],
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
# API: /api/live_quotes  (2-second TTL — powers blinking live prices)
# ---------------------------------------------------------------------------
def _fetch_live_quotes():
    open_pos = read_positions("OPEN")
    pos_tickers = list(set(p["ticker"] for p in open_pos))
    market_syms = ["^GSPC", "^IXIC", "^DJI", "^RUT", "XLE", "XLF", "XLK", "XLV", "UUP"]
    all_equity_syms = list(set(market_syms + pos_tickers))
    quotes = {}
    # Primary: FMP /stable/batch-quote (correct endpoint post-Aug 2025)
    try:
        sym_str = ",".join(all_equity_syms).replace("^", "%5E")
        r = requests.get(
            "https://financialmodelingprep.com/stable/batch-quote?symbols=" + sym_str + "&apikey=" + config.FMP_API_KEY,
            timeout=6
        )
        data = r.json()
        if isinstance(data, list):
            for q in data:
                sym = q.get("symbol", "")
                price = q.get("price")
                chg = q.get("changePercentage")  # NOTE: new field name vs old v3
                quotes[sym] = {
                    "price": round(float(price), 2) if price else None,
                    "change_pct": round(float(chg), 2) if chg is not None else None,
                    "change": round(float(q.get("change", 0) or 0), 2),
                    "source": "fmp",
                }
            log.info(f"Live quotes FMP stable: {len(quotes)} symbols")
    except Exception as e:
        log.warning("Live quotes FMP failed: " + str(e))
    # Crypto: FMP stable batch
    try:
        crypto_map = {"BTCUSD": "BTCUSD", "ETHUSD": "ETHUSD", "SOLUSD": "SOLUSD"}
        cr = requests.get(
            "https://financialmodelingprep.com/stable/batch-quote?symbols=BTCUSD,ETHUSD,SOLUSD&apikey=" + config.FMP_API_KEY,
            timeout=4
        )
        cdata = cr.json()
        if isinstance(cdata, list):
            for q in cdata:
                sym = q.get("symbol", "")
                price = q.get("price")
                chg = q.get("changePercentage")
                quotes[sym] = {
                    "price": round(float(price), 2) if price else None,
                    "change_pct": round(float(chg), 2) if chg is not None else None,
                    "source": "fmp",
                }
    except Exception:
        pass
    # Vol indexes + 10Y yield + VIX3M term structure (yfinance — FMP unreliable)
    try:
        vol = batch_fetch_prices(["^VIX", "^OVX", "^GVZ", "^TNX", "^VIX3M"])
        for sym, tk in [("VIX", "^VIX"), ("OVX", "^OVX"), ("GVZ", "^GVZ"),
                        ("TNX", "^TNX"), ("VIX3M", "^VIX3M")]:
            if tk in vol and vol[tk]:
                quotes[sym] = {"price": round(vol[tk], 2), "change_pct": None, "change": None, "source": "yf"}
    except Exception:
        pass
    return {"quotes": quotes, "ts": time.time(), "stale": not quotes, "source": "fmp"}

def _fetch_news():
    try:
        r = requests.get(
            "https://financialmodelingprep.com/api/v3/stock_news?limit=25&apikey=" + config.FMP_API_KEY,
            timeout=8
        )
        items = r.json()
        if isinstance(items, list):
            return [{"title": i.get("title", ""), "symbol": i.get("symbol", ""), "ts": i.get("publishedDate", "")} for i in items[:25]]
    except Exception as e:
        log.warning("News fetch failed: " + str(e))
    return []


def _calc_realized_vol(closes):
    """Calculate 20d and 60d annualized realized vol from daily close prices.
    Returns (vol_20d, vol_60d, ratio) or (None, None, None)."""
    import math
    if len(closes) < 21:
        return None, None, None
    log_rets = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes))]

    def _std(arr):
        n = len(arr)
        if n < 2:
            return 0.0
        mean = sum(arr) / n
        return (sum((x - mean) ** 2 for x in arr) / (n - 1)) ** 0.5

    vol_20 = _std(log_rets[-20:]) * (252 ** 0.5) * 100
    rets_60 = log_rets[-60:] if len(log_rets) >= 60 else log_rets
    vol_60 = _std(rets_60) * (252 ** 0.5) * 100
    ratio = round(vol_20 / vol_60, 2) if vol_60 > 0 else None
    return round(vol_20, 2), round(vol_60, 2), ratio


def _fetch_breadth_vol():
    """Breadth & volatility snapshot: SPY realized vol vs 60d avg, VIX term structure, put/call ratio."""
    out = {"spy_vol_today": None, "spy_vol_30d_avg": None, "spy_vol_ratio": None,
           "vix": None, "vix3m": None, "term_ratio": None, "term_state": None,
           "put_call": None, "stale": True}

    # --- SPY Realized Vol: FMP primary, yfinance fallback ---
    try:
        from_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
        url = (f"https://financialmodelingprep.com/stable/historical-price-eod/full"
               f"?symbol=SPY&from={from_date}&apikey={config.FMP_API_KEY}")
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, dict):
                data = data.get("historical", [])
            if isinstance(data, list) and data:
                data.sort(key=lambda x: x.get("date", ""))
                closes = [float(d["close"]) for d in data if d.get("close")]
                v20, v60, ratio = _calc_realized_vol(closes)
                if v20 is not None:
                    out["spy_vol_today"] = v20
                    out["spy_vol_30d_avg"] = v60
                    out["spy_vol_ratio"] = ratio
    except Exception as e:
        log.warning(f"SPY vol FMP failed: {e}")

    if out["spy_vol_today"] is None:
        try:
            import pandas
            df = yf.download("SPY", period="90d", interval="1d", progress=False)
            if df is not None and not df.empty:
                if isinstance(df.columns, pandas.MultiIndex):
                    df.columns = [c[0] for c in df.columns]
                closes = [float(c) for c in df["Close"].dropna().values]
                v20, v60, ratio = _calc_realized_vol(closes)
                if v20 is not None:
                    out["spy_vol_today"] = v20
                    out["spy_vol_30d_avg"] = v60
                    out["spy_vol_ratio"] = ratio
        except Exception as e:
            log.warning(f"SPY vol yfinance fallback failed: {e}")

    # --- VIX Term Structure (keep existing approach) ---
    try:
        vol = batch_fetch_prices(["^VIX", "^VIX3M"])
        vix = vol.get("^VIX")
        vix3m = vol.get("^VIX3M")
        out["vix"] = round(vix, 2) if vix else None
        out["vix3m"] = round(vix3m, 2) if vix3m else None
        if vix and vix3m:
            ratio = vix / vix3m
            out["term_ratio"] = round(ratio, 3)
            out["term_state"] = "CONTANGO" if ratio < 1 else "BACKWARDATION"
    except Exception as e:
        log.warning("Breadth/vol term calc failed: " + str(e))

    # --- Put/Call Ratio: CBOE primary, alternate fallback ---
    try:
        r = requests.get(
            "https://www.cboe.com/data/volatility-index-values/cboe-options-statistics/",
            headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        if r.status_code == 200:
            import re
            m = re.search(r'(?:Total|Equity)\s*Put/Call\s*Ratio[^0-9]*([\d]+\.[\d]+)', r.text, re.IGNORECASE)
            if m:
                out["put_call"] = round(float(m.group(1)), 2)
    except Exception:
        pass
    if out["put_call"] is None:
        try:
            r = requests.get(
                "https://markets.cboe.com/us/options/market_statistics/daily/",
                headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
            if r.status_code == 200:
                import re
                m = re.search(r'put.?call.*?([\d]+\.[\d]+)', r.text, re.IGNORECASE)
                if m:
                    out["put_call"] = round(float(m.group(1)), 2)
        except Exception:
            pass

    out["stale"] = out["spy_vol_today"] is None and out["vix"] is None
    return out

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/api/live_quotes")
def api_live_quotes():
    return jsonify(cached("live_quotes", 2, _fetch_live_quotes))

@app.route("/api/news")
def api_news():
    return jsonify(cached("news", 60, _fetch_news))

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


@app.route("/api/breadth_vol")
def api_breadth_vol():
    return jsonify(cached("breadth_vol", 300, _fetch_breadth_vol))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    log.info(f"GMC Dashboard starting on {config.DASHBOARD_HOST}:{config.DASHBOARD_PORT}")
    app.run(host=config.DASHBOARD_HOST, port=config.DASHBOARD_PORT, debug=False)
