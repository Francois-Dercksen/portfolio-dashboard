from flask import Flask, jsonify, request, Response, stream_with_context, send_file
from flask_cors import CORS
import yfinance as yf
import csv, io, json, time
from datetime import datetime, timedelta

app = Flask(__name__)
CORS(app)

REQUIRED_COLUMNS = {"ticker", "weight", "price"}

@app.route('/')
def serve_index():
    return send_file('index.html')

def safe_get(obj, attr, default=None):
    """Safely get an attribute, catching any subscript/type errors."""
    try:
        val = getattr(obj, attr, default)
        return val if val is not None else default
    except Exception:
        return default


def fetch_ticker_data(ticker: str, retries: int = 2):
    for attempt in range(retries + 1):
        try:
            stock = yf.Ticker(ticker.upper())

            # Each attribute access wrapped individually
            try:
                fast = stock.fast_info
            except Exception as e:
                if attempt < retries:
                    time.sleep(1.5)
                    continue
                return None, f"Could not load ticker: {e}"

            price      = safe_get(fast, "last_price")
            prev_close = safe_get(fast, "previous_close")
            currency   = safe_get(fast, "currency", "USD") or "USD"

            if price is None or prev_close is None:
                if attempt < retries:
                    time.sleep(1.5)
                    continue
                return None, "Price unavailable — check the ticker symbol."

            try:
                price      = float(price)
                prev_close = float(prev_close)
            except (TypeError, ValueError) as e:
                if attempt < retries:
                    time.sleep(1.5)
                    continue
                return None, f"Invalid price data: {e}"

            change     = price - prev_close
            change_pct = (change / prev_close) * 100

            # history — fully isolated
            history_dates, history_closes = [], []
            try:
                hist = stock.history(period="30d")
                if hist is not None and not hist.empty:
                    history_dates  = [str(d.date()) for d in hist.index.tolist()]
                    history_closes = [round(float(v), 2) for v in hist["Close"].dropna().tolist()]
            except Exception:
                pass

            # name — each attr isolated
            name = ticker.upper()
            for attr in ("short_name", "long_name"):
                try:
                    val = getattr(fast, attr, None)
                    if val and isinstance(val, str) and val.strip():
                        name = val.strip()
                        break
                except Exception:
                    continue

            return {
                "ticker":     ticker.upper(),
                "name":       name,
                "price":      round(price, 2),
                "change":     round(change, 2),
                "change_pct": round(change_pct, 2),
                "currency":   str(currency),
                "history":    history_closes,
                "dates":      history_dates,
            }, None

        except Exception as e:
            if attempt < retries:
                time.sleep(1.5)
                continue
            return None, str(e)

    return None, "Max retries exceeded."


def parse_csv(raw: str):
    reader = csv.DictReader(io.StringIO(raw.strip()))
    rows   = list(reader)
    if not rows:
        return None, "CSV is empty."

    headers = {c.lower() for c in (reader.fieldnames or [])}
    missing = REQUIRED_COLUMNS - headers
    if missing:
        return None, f"CSV is missing required column(s): {', '.join(sorted(missing))}."

    rows = [{k.lower(): v for k, v in r.items()} for r in rows]
    seen = {}
    for r in rows:
        t = r["ticker"].strip().upper()
        if t and t not in seen:
            seen[t] = r
    return seen, None


def safe_float(val):
    try:
        return float(val) if val not in (None, "") else None
    except (ValueError, TypeError):
        return None


def enrich(data: dict, meta: dict) -> dict:
    data["weight"]      = safe_float(meta.get("weight"))
    data["start_price"] = safe_float(meta.get("price"))
    data["quarter"]     = meta.get("quarter", "")
    data["cik"]         = meta.get("cik", "")
    return data


@app.route("/api/stock/<ticker>")
def get_stock(ticker):
    try:
        data, err = fetch_ticker_data(ticker)
        if err:
            return jsonify({"error": err}), 400
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/benchmark")
def get_benchmark():
    start_date_str = request.args.get("start_date", "")
    if not start_date_str:
        return jsonify({"error": "start_date is required (YYYY-MM-DD)."}), 400
    try:
        start_dt = datetime.strptime(start_date_str, "%Y-%m-%d")
    except ValueError:
        return jsonify({"error": "Invalid date format. Use YYYY-MM-DD."}), 400

    try:
        spx = yf.Ticker("^GSPC")
        fast       = spx.fast_info
        current    = fast.last_price
        prev_close = fast.previous_close
        if current is None:
            return jsonify({"error": "Could not fetch SPX current price."}), 500

        change     = current - prev_close
        change_pct = (change / prev_close) * 100

        window_end = start_dt + timedelta(days=10)
        hist = spx.history(start=start_dt.strftime("%Y-%m-%d"),
                           end=window_end.strftime("%Y-%m-%d"))
        if hist.empty:
            return jsonify({"error": f"No SPX data found on or after {start_date_str}."}), 404

        start_price = round(float(hist["Close"].iloc[0]), 2)
        actual_date = str(hist.index[0].date())
        ret = ((current - start_price) / start_price) * 100

        hist30 = spx.history(period="30d")
        history_dates  = [str(d.date()) for d in hist30.index.tolist()]
        history_closes = [round(float(v), 2) for v in hist30["Close"].dropna().tolist()]

        return jsonify({
            "ticker":      "SPX",
            "name":        "S&P 500",
            "start_date":  actual_date,
            "start_price": start_price,
            "price":       round(current, 2),
            "change":      round(change, 2),
            "change_pct":  round(change_pct, 2),
            "return":      round(ret, 2),
            "currency":    "USD",
            "history":     history_closes,
            "dates":       history_dates,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/portfolio/stream", methods=["POST"])
def portfolio_stream():
    if "file" in request.files:
        raw = request.files["file"].read().decode("utf-8")
    else:
        raw = request.get_data(as_text=True)

    seen, err = parse_csv(raw)
    if err:
        def error_stream():
            yield f"data: {json.dumps({'type': 'error', 'message': err})}\n\n"
        return Response(stream_with_context(error_stream()), mimetype="text/event-stream")

    total = len(seen)

    def generate():
        holdings, errors = [], []
        idx = 0
        for ticker, meta in seen.items():
            idx += 1
            try:
                data, fetch_err = fetch_ticker_data(ticker)
                if fetch_err:
                    errors.append({"ticker": ticker, "error": fetch_err})
                    yield f"data: {json.dumps({'type':'progress','done':idx,'total':total,'ticker':ticker,'status':'error','error':fetch_err})}\n\n"
                    continue
                data = enrich(data, meta)
                holdings.append(data)
                yield f"data: {json.dumps({'type':'progress','done':idx,'total':total,'ticker':ticker,'status':'ok','holding':data})}\n\n"
            except Exception as e:
                msg = str(e)
                errors.append({"ticker": ticker, "error": msg})
                yield f"data: {json.dumps({'type':'progress','done':idx,'total':total,'ticker':ticker,'status':'error','error':msg})}\n\n"

            # small pause to avoid hammering Yahoo Finance rate limits
            time.sleep(0.15)

        yield f"data: {json.dumps({'type':'done','holdings':holdings,'errors':errors})}\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )


@app.route("/api/portfolio", methods=["POST"])
def get_portfolio():
    try:
        if "file" in request.files:
            raw = request.files["file"].read().decode("utf-8")
        else:
            raw = request.get_data(as_text=True)
        seen, err = parse_csv(raw)
        if err:
            return jsonify({"error": err}), 400
        results, errors = [], []
        for ticker, meta in seen.items():
            try:
                data, fetch_err = fetch_ticker_data(ticker)
                if fetch_err:
                    errors.append({"ticker": ticker, "error": fetch_err})
                    continue
                results.append(enrich(data, meta))
            except Exception as e:
                errors.append({"ticker": ticker, "error": str(e)})
            time.sleep(0.15)
        return jsonify({"holdings": results, "errors": errors})
    except Exception as e:
        return jsonify({"error": str(e)}), 500



@app.route("/api/charts")
def get_charts():
    """
    Returns historical price data from start_date to today for:
    - ^GSPC (SPX benchmark)
    - up to 10 tickers passed as comma-separated ?tickers=AAPL,MSFT,...
    Each series is normalised to 100 at start_date so they're comparable.
    Also returns raw prices for portfolio return calculation using weights.
    Query params:
      start_date   YYYY-MM-DD   required
      tickers      comma-sep    required  (ordered by weight, top 10)
      weights      comma-sep    required  (matching weights for portfolio line)
      start_prices comma-sep    required  (CSV start prices per ticker)
    """
    start_date_str = request.args.get("start_date", "")
    tickers_str    = request.args.get("tickers", "")
    weights_str    = request.args.get("weights", "")
    starts_str     = request.args.get("start_prices", "")

    if not start_date_str:
        return jsonify({"error": "start_date required"}), 400
    if not tickers_str:
        return jsonify({"error": "tickers required"}), 400

    try:
        start_dt = datetime.strptime(start_date_str, "%Y-%m-%d")
    except ValueError:
        return jsonify({"error": "Invalid date format"}), 400

    tickers      = [t.strip().upper() for t in tickers_str.split(",") if t.strip()][:10]
    weights      = [safe_float(w) for w in weights_str.split(",")]
    start_prices = [safe_float(p) for p in starts_str.split(",")]

    # Pad weights / start_prices if shorter than tickers
    while len(weights)      < len(tickers): weights.append(None)
    while len(start_prices) < len(tickers): start_prices.append(None)

    end_str   = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    start_str = start_dt.strftime("%Y-%m-%d")

    results = {}

    # ── fetch each ticker ────────────────────────────────────────────────────
    for ticker in tickers:
        try:
            hist = yf.Ticker(ticker).history(start=start_str, end=end_str)
            if hist is None or hist.empty:
                results[ticker] = {"dates": [], "values": [], "error": "No data"}
                continue
            dates  = [str(d.date()) for d in hist.index]
            closes = [round(float(v), 4) for v in hist["Close"].dropna()]
            results[ticker] = {"dates": dates, "values": closes}
        except Exception as e:
            results[ticker] = {"dates": [], "values": [], "error": str(e)}
        time.sleep(0.1)

    # ── SPX benchmark ────────────────────────────────────────────────────────
    try:
        spx_hist = yf.Ticker("^GSPC").history(start=start_str, end=end_str)
        spx_dates  = [str(d.date()) for d in spx_hist.index]
        spx_values = [round(float(v), 4) for v in spx_hist["Close"].dropna()]
    except Exception:
        spx_dates, spx_values = [], []

    # ── build portfolio return series ────────────────────────────────────────
    # Align all ticker series to a common date spine using SPX dates as reference
    # Portfolio daily value = sum(weight_i * price_i / start_price_i) normalised
    # Use only tickers that have start_prices and weights
    port_dates  = spx_dates[:]
    port_values = []

    valid = [(tickers[i], weights[i], start_prices[i])
             for i in range(len(tickers))
             if weights[i] is not None and start_prices[i] is not None
             and tickers[i] in results and results[tickers[i]]["dates"]]

    if valid and port_dates:
        # Build date→price map for each valid ticker
        price_maps = {}
        for t, w, sp in valid:
            price_maps[t] = dict(zip(results[t]["dates"], results[t]["values"]))

        # Total weight across ALL valid tickers (fixed denominator)
        total_weight = sum(w for _, w, _ in valid)

        for date in port_dates:
            if total_weight <= 0:
                port_values.append(None)
                continue
            weighted_ret = 0.0
            covered_w    = 0.0
            for t, w, sp in valid:
                p = price_maps[t].get(date)
                if p is not None and sp is not None and sp > 0:
                    # individual return * weight
                    weighted_ret += ((p / sp) - 1.0) * w
                    covered_w    += w
            if covered_w / total_weight >= 0.5:
                # scale to % and index to 100
                port_values.append(round(100 + (weighted_ret / total_weight) * 100, 4))
            else:
                port_values.append(None)

    return jsonify({
        "portfolio": {"dates": port_dates, "values": port_values},
        "benchmark": {"dates": spx_dates,  "values": spx_values},
        "holdings":  results,
        "tickers":   tickers,
        "weights":   weights,
        "start_prices": start_prices,
    })



@app.route("/api/fundamentals/<ticker>")
def get_fundamentals(ticker):
    try:
        stock = yf.Ticker(ticker.upper())

        def sf(val):
            try: return float(val) if val not in (None, '') else None
            except: return None

        # Try fast_info first for market data
        fast = None
        try:
            fast = stock.fast_info
        except Exception:
            pass

        # Try stock.info with retries — primary source for PE and beta
        info = {}
        for attempt in range(3):
            try:
                raw = stock.info
                if isinstance(raw, dict) and len(raw) > 5:
                    info = raw
                    break
            except Exception:
                pass
            time.sleep(1)

        # PE: try multiple keys in order of preference
        pe = None
        for key in ('trailingPE', 'forwardPE', 'trailingEps'):
            val = sf(info.get(key))
            if val is not None and val > 0:
                pe = val
                break

        # Beta: try info first, then fast_info
        beta = sf(info.get('beta'))
        if beta is None and fast is not None:
            beta = sf(safe_get(fast, 'beta'))

        # Volume and market cap
        volume  = sf(safe_get(fast, 'three_month_average_volume') if fast else None)                   or sf(info.get('averageVolume'))                   or sf(info.get('volume'))
        mkt_cap = sf(safe_get(fast, 'market_cap') if fast else None)                   or sf(info.get('marketCap'))

        return jsonify({
            'ticker':  ticker.upper(),
            'pe':      round(pe, 2)      if pe      is not None else None,
            'beta':    round(beta, 2)    if beta    is not None else None,
            'volume':  int(volume)       if volume  is not None else None,
            'mktCap':  int(mkt_cap)      if mkt_cap is not None else None,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route("/api/portfolio/auto")
def portfolio_auto():
    startdate = request.args.get("startdate", "")
    if not startdate:
        return jsonify({"error": "startdate is required"}), 400
    try:
        with open("portfolio.csv", "r") as f:
            raw = f.read()
        seen, err = parse_csv(raw)
        if err:
            return jsonify({"error": err}), 400
        results, errors = [], []
        for ticker, meta in seen.items():
            try:
                data, fetch_err = fetch_ticker_data(ticker)
                if fetch_err:
                    errors.append({"ticker": ticker, "error": fetch_err})
                    continue
                results.append(enrich(data, meta))
            except Exception as e:
                errors.append({"ticker": ticker, "error": str(e)})
            time.sleep(0.15)
        return jsonify({"holdings": results, "errors": errors})
    except FileNotFoundError:
        return jsonify({"error": "portfolio.csv not found in repository"}), 404


if __name__ == "__main__":
    import os
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
