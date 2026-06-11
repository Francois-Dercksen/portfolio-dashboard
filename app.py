from flask import Flask, jsonify, request, Response, send_file
from flask_cors import CORS
from curl_cffi import requests as curl_requests
import yfinance as yf
import csv, io, json, time, os

app = Flask(__name__)
CORS(app)

# Shared browser-impersonating session — bypasses Yahoo Finance TLS fingerprinting
_session = curl_requests.Session(impersonate="chrome")


@app.route('/')
def serve_index():
    return send_file('index.html')


# ── helpers ────────────────────────────────────────────────────────────────────

def fetch_price_data(ticker: str):
    stock = yf.Ticker(ticker.upper(), session=_session)
    fast = stock.fast_info
    price = fast.last_price
    prev_close = fast.previous_close
    if price is None or prev_close is None:
        return None, "Could not fetch price"
    change = price - prev_close
    change_pct = (change / prev_close) * 100
    return {
        "ticker":     ticker.upper(),
        "name":       stock.info.get("shortName", ticker.upper()),
        "price":      round(price, 4),
        "change":     round(change, 4),
        "change_pct": round(change_pct, 4),
        "currency":   fast.currency or "USD",
    }, None


# ── single stock ───────────────────────────────────────────────────────────────

@app.route("/api/stock/<ticker>")
def get_stock(ticker):
    try:
        data, err = fetch_price_data(ticker)
        if err:
            return jsonify({"error": err}), 400
        stock = yf.Ticker(ticker.upper(), session=_session)
        hist = stock.history(period="30d")
        data["history"] = [round(float(v), 2) for v in hist["Close"].dropna().tolist()]
        data["dates"]   = [str(d.date()) for d in hist.index.tolist()]
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── benchmark ──────────────────────────────────────────────────────────────────

@app.route("/api/benchmark")
def get_benchmark():
    try:
        start_date = request.args.get("start_date", "")
        spx = yf.Ticker("^GSPC", session=_session)
        hist = spx.history(start=start_date) if start_date else spx.history(period="1mo")
        if hist.empty:
            return jsonify({"error": "No SPX data"}), 400
        closes = hist["Close"].dropna()
        start_price = float(closes.iloc[0])
        end_price   = float(closes.iloc[-1])
        ret = (end_price / start_price - 1) * 100
        return jsonify({
            "return":     round(ret, 4),
            "start_date": str(closes.index[0].date()),
            "end_date":   str(closes.index[-1].date()),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── charts ─────────────────────────────────────────────────────────────────────

@app.route("/api/charts")
def get_charts():
    try:
        start_date   = request.args.get("start_date", "")
        tickers      = request.args.get("tickers", "").split(",")
        weights      = [float(w) for w in request.args.get("weights", "").split(",")]
        start_prices = []
        for sp in request.args.get("start_prices", "").split(","):
            try:    start_prices.append(float(sp))
            except: start_prices.append(None)

        tickers = [t.strip().upper() for t in tickers if t.strip()]
        if not tickers:
            return jsonify({"error": "No tickers provided"}), 400

        holdings_series = {}
        all_dates_set = set()

        for ticker in tickers:
            try:
                hist = yf.Ticker(ticker, session=_session).history(start=start_date)
                if hist.empty:
                    holdings_series[ticker] = {"dates": [], "values": []}
                    continue
                closes = hist["Close"].dropna()
                dates  = [str(d.date()) for d in closes.index.tolist()]
                values = [round(float(v), 4) for v in closes.tolist()]
                holdings_series[ticker] = {"dates": dates, "values": values}
                all_dates_set.update(dates)
            except:
                holdings_series[ticker] = {"dates": [], "values": []}

        # benchmark (SPX)
        try:
            bh   = yf.Ticker("^GSPC", session=_session).history(start=start_date)
            bc   = bh["Close"].dropna()
            b_dates  = [str(d.date()) for d in bc.index.tolist()]
            b_values = [round(float(v), 4) for v in bc.tolist()]
        except:
            b_dates, b_values = [], []

        # weighted portfolio index (base 100)
        all_dates = sorted(all_dates_set)
        port_values = []
        for date in all_dates:
            idx_val = 100.0
            for i, ticker in enumerate(tickers):
                s = holdings_series.get(ticker, {})
                if date not in s.get("dates", []):
                    continue
                d_idx = s["dates"].index(date)
                cur   = s["values"][d_idx]
                sp    = start_prices[i] if i < len(start_prices) and start_prices[i] else None
                w     = weights[i]      if i < len(weights) else 0
                if sp and sp > 0:
                    idx_val += (cur / sp - 1) * 100 * w
            port_values.append(round(idx_val, 4))

        return jsonify({
            "tickers":      tickers,
            "weights":      weights,
            "start_prices": start_prices,
            "holdings":     holdings_series,
            "portfolio":    {"dates": all_dates, "values": port_values},
            "benchmark":    {"dates": b_dates,   "values": b_values},
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── fundamentals ───────────────────────────────────────────────────────────────

@app.route("/api/fundamentals/<ticker>")
def get_fundamentals(ticker):
    try:
        info = yf.Ticker(ticker.upper(), session=_session).info
        return jsonify({
            "pe":     info.get("trailingPE"),
            "beta":   info.get("beta"),
            "volume": info.get("volume"),
            "mktCap": info.get("marketCap"),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── portfolio stream ───────────────────────────────────────────────────────────

@app.route("/api/portfolio/stream", methods=["POST"])
def portfolio_stream():
    try:
        if "file" in request.files:
            raw = request.files["file"].read().decode("utf-8")
        else:
            raw = request.get_data(as_text=True)

        reader = csv.DictReader(io.StringIO(raw.strip()))
        rows   = list(reader)

        if not rows:
            return jsonify({"error": "CSV is empty."}), 400

        cols = [c.lower().strip() for c in (reader.fieldnames or [])]
        for req_col in ("ticker", "weight", "price"):
            if req_col not in cols:
                return jsonify({"error": f"CSV missing required column: {req_col}"}), 400

        rows  = [{k.lower().strip(): v for k, v in r.items()} for r in rows]
        total = len(rows)

    except Exception as e:
        return jsonify({"error": str(e)}), 400

    def generate():
        holdings = []
        errors   = []

        for i, row in enumerate(rows):
            ticker = row.get("ticker", "").strip().upper()
            if not ticker:
                continue
            try:
                data, err = fetch_price_data(ticker)
                if err:
                    raise ValueError(err)
                data["weight"]      = float(row["weight"]) if row.get("weight") else None
                data["start_price"] = float(row["price"])  if row.get("price")  else None
                data["quarter"]     = row.get("quarter", "")
                data["cik"]         = row.get("cik", "")
                holdings.append(data)
                msg = {"type": "progress", "done": i+1, "total": total,
                       "ticker": ticker, "status": "ok", "holding": data}
            except Exception as e:
                errors.append({"ticker": ticker, "error": str(e)})
                msg = {"type": "progress", "done": i+1, "total": total,
                       "ticker": ticker, "status": "error", "error": str(e)}

            yield f"data: {json.dumps(msg)}\n\n"
            time.sleep(0.05)

        done_msg = {"type": "done", "holdings": holdings, "errors": errors}
        yield f"data: {json.dumps(done_msg)}\n\n"

    return Response(generate(), mimetype="text/event-stream",
                    headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"})


# ── default portfolio ──────────────────────────────────────────────────────────

@app.route("/api/default-portfolio")
def default_portfolio():
    csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "portfolio.csv")
    if not os.path.exists(csv_path):
        return jsonify({"error": "portfolio.csv not found next to app.py"}), 404
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            raw = f.read()
        return raw, 200, {"Content-Type": "text/plain; charset=utf-8"}
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)
