#!/usr/bin/env python3
"""BINANCE GEMS - early pump coin selector (Binance Spot, USDT pairs).

Telegram: detailed signal.  Binance Square: 2-4 line post, template depends on coin type.
Gap of 10-15 sec between every post.
"""
import os, time, json, random, statistics
import requests

BASES = ["https://data-api.binance.vision", "https://api.binance.com", "https://api1.binance.com"]
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID")
SQ_KEY = os.getenv("BINANCE_SQUARE_API_KEY")  # optional
SQ_URL = "https://www.binance.com/bapi/composite/v1/public/pgc/openApi/content/add"

STATE_FILE = "sent_state.json"
COOLDOWN_H = 12          # same coin won't be posted again within this time
MAX_SIGNALS = 4          # max signals per run
MIN_SCORE = 55           # raise for fewer/stronger signals
MIN_QV, MAX_QV = 500_000, 150_000_000   # 24h USDT volume range
GAP = (10, 15)           # seconds between posts

STABLES = {"USDC", "FDUSD", "TUSD", "USDP", "DAI", "EUR", "AEUR", "USDE", "XUSD", "BFUSD", "PAXG"}


# ---------------- helpers ----------------
def api(path, params=None):
    for base in BASES:
        try:
            r = requests.get(base + path, params=params, timeout=15)
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
    return None


def ema(vals, n):
    k = 2 / (n + 1)
    e = vals[0]
    for v in vals[1:]:
        e = v * k + e * (1 - k)
    return e


def rsi(closes, n=14):
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0)); losses.append(max(-d, 0))
    ag, al = sum(gains[:n]) / n, sum(losses[:n]) / n
    for i in range(n, len(gains)):
        ag = (ag * (n - 1) + gains[i]) / n
        al = (al * (n - 1) + losses[i]) / n
    return 100.0 if al == 0 else 100 - 100 / (1 + ag / al)


def bandwidth(window):
    m = sum(window) / len(window)
    return 4 * statistics.pstdev(window) / m if m else 0


def fmt_price(p):
    if p >= 100: return f"{p:,.2f}"
    if p >= 1: return f"{p:.4f}"
    if p >= 0.01: return f"{p:.5f}"
    return f"{p:.8f}"


def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(s):
    now = time.time()
    s = {k: v for k, v in s.items() if now - v < 48 * 3600}
    with open(STATE_FILE, "w") as f:
        json.dump(s, f)


# ---------------- analysis ----------------
def analyze(sym):
    k15 = api("/api/v3/klines", {"symbol": sym, "interval": "15m", "limit": 100})
    k1h = api("/api/v3/klines", {"symbol": sym, "interval": "1h", "limit": 80})
    if not k15 or not k1h or len(k15) < 60 or len(k1h) < 60:
        return None
    k15, k1h = k15[:-1], k1h[:-1]  # closed candles only

    c15 = [float(x[4]) for x in k15]
    h15 = [float(x[2]) for x in k15]
    v15 = [float(x[5]) for x in k15]
    tb15 = [float(x[9]) for x in k15]
    c1h = [float(x[4]) for x in k1h]

    base_vol = statistics.mean(v15[-22:-2]) or 1e-9
    vol_ratio = ((v15[-1] + v15[-2]) / 2) / base_vol
    change_1h = (c15[-1] / c15[-5] - 1) * 100
    change_4h = (c1h[-1] / c1h[-5] - 1) * 100
    r = rsi(c15)
    tv = sum(v15[-4:]) or 1e-9
    taker = sum(tb15[-4:]) / tv
    breakout = c15[-1] > max(h15[-25:-1]) and change_1h < 6

    bws = [bandwidth(c1h[i - 20:i]) for i in range(len(c1h) - 40, len(c1h) + 1)]
    squeeze = bws[-1] <= sorted(bws)[len(bws) // 4]

    e20, e50 = ema(c1h[-60:], 20), ema(c1h[-60:], 50)
    trend_up = c1h[-1] > e20 and e20 > e50

    score = 0
    score += 30 if vol_ratio >= 4 else 25 if vol_ratio >= 3 else 15 if vol_ratio >= 2 else 0
    if 0.5 <= change_1h <= 4: score += 20
    elif 0 <= change_1h < 0.5: score += 10
    elif change_1h > 5: score -= 20
    score += 15 if taker > 0.58 else 8 if taker > 0.53 else 0
    if 50 <= r <= 68: score += 10
    elif r > 75: score -= 20
    if squeeze: score += 15
    if breakout: score += 15
    if trend_up: score += 10

    # coin type decides the post style
    if breakout and vol_ratio >= 2:
        ctype = "BREAKOUT"
    elif change_4h < -3 and change_1h > 0.3 and r < 55:
        ctype = "REVERSAL"
    elif squeeze and vol_ratio >= 1.5:
        ctype = "SQUEEZE"
    elif taker > 0.56 and abs(change_1h) < 1.5 and vol_ratio >= 2:
        ctype = "ACCUMULATION"
    else:
        ctype = "MOMENTUM"

    return dict(symbol=sym, base=sym[:-4], price=c15[-1], score=score, type=ctype,
                vol_ratio=vol_ratio, ch1h=change_1h, ch4h=change_4h, rsi=r,
                taker=taker * 100, squeeze=squeeze, breakout=breakout)


def scan():
    tick = api("/api/v3/ticker/24hr") or []
    cands = []
    for t in tick:
        s = t["symbol"]
        if not s.endswith("USDT"):
            continue
        b = s[:-4]
        if b in STABLES or b.endswith(("UP", "DOWN", "BULL", "BEAR")):
            continue
        qv, ch = float(t["quoteVolume"]), float(t["priceChangePercent"])
        if MIN_QV <= qv <= MAX_QV and -8 <= ch <= 12:   # not already pumped
            cands.append((qv, s))
    cands.sort(reverse=True)
    out = []
    for _, s in cands[:130]:
        try:
            a = analyze(s)
            if a and a["score"] >= MIN_SCORE:
                out.append(a)
        except Exception as e:
            print("skip", s, e)
        time.sleep(0.08)
    out.sort(key=lambda x: x["score"], reverse=True)
    return out


# ---------------- messages ----------------
def telegram_text(a):
    icons = {"BREAKOUT": "🚀", "REVERSAL": "🔄", "SQUEEZE": "🧨", "ACCUMULATION": "🐋", "MOMENTUM": "⚡"}
    return (
        f"{icons[a['type']]} EARLY SIGNAL: #{a['base']}/USDT  [{a['type']}]\n"
        f"Price: {fmt_price(a['price'])}\n"
        f"Score: {a['score']}\n"
        f"Volume: {a['vol_ratio']:.1f}x normal\n"
        f"Change: 1h {a['ch1h']:+.2f}% | 4h {a['ch4h']:+.2f}%\n"
        f"RSI(15m): {a['rsi']:.0f} | Buyers: {a['taker']:.0f}%\n"
        f"Squeeze: {'yes' if a['squeeze'] else 'no'} | Range breakout: {'yes' if a['breakout'] else 'no'}\n\n"
        f"Early detection, not a guarantee. Use stop-loss. DYOR."
    )


def square_text(a):
    b, p = a["base"], fmt_price(a["price"])
    vr, tk, c1 = f"{a['vol_ratio']:.1f}", f"{a['taker']:.0f}", f"{a['ch1h']:+.1f}"
    T = {
        "BREAKOUT": [
            f"🚀 ${b} just broke its recent range\nPrice {p} | Volume {vr}x normal\nBuyers in control ({tk}% taker buy)\nDYOR, not financial advice",
            f"${b} breakout alert 🔥\nPushing above resistance at {p}\nVolume running {vr}x above average\nNot financial advice",
        ],
        "REVERSAL": [
            f"🔄 ${b} bouncing after a dip\nPrice {p}, 1h {c1}%\nBuy volume is picking up ({vr}x)\nDYOR, not financial advice",
            f"${b} showing reversal signs 👀\nDip is getting bought around {p}\nRSI {a['rsi']:.0f}, volume {vr}x\nNot financial advice",
        ],
        "SQUEEZE": [
            f"🧨 ${b} is coiling tight\nVolatility squeezed, volume rising ({vr}x)\nWatching {p} for the move\nDYOR, not financial advice",
            f"${b} compression setup ⏳\nQuiet price at {p} but volume is waking up\nBig move could be near\nNot financial advice",
        ],
        "ACCUMULATION": [
            f"🐋 Quiet accumulation on ${b}\nPrice flat at {p}, volume {vr}x\n{tk}% of trades are buys\nDYOR, not financial advice",
            f"${b} looks like it's being accumulated 👀\nPrice steady ({c1}% 1h) while buying grows\nWatching {p}\nNot financial advice",
        ],
        "MOMENTUM": [
            f"⚡ ${b} momentum building\nPrice {p}, 1h {c1}%\nVolume {vr}x normal, buyers {tk}%\nDYOR, not financial advice",
            f"${b} early momentum spotted 📈\nTrading at {p} with rising volume\nStill early, manage risk\nNot financial advice",
        ],
    }
    return random.choice(T[a["type"]])


# ---------------- sending ----------------
def send_telegram(text):
    if not TG_TOKEN or not TG_CHAT:
        print("[dry-run telegram]\n" + text)
        return True
    try:
        r = requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                          json={"chat_id": TG_CHAT, "text": text}, timeout=20)
        ok = r.status_code == 200
        if not ok: print("telegram error", r.text[:200])
        return ok
    except Exception as e:
        print("telegram exception", e)
        return False


def send_square(text):
    if not SQ_KEY:
        print("[square skipped - no BINANCE_SQUARE_API_KEY]\n" + text)
        return False
    try:
        r = requests.post(SQ_URL, json={"bodyTextOnly": text}, timeout=20,
                          headers={"X-Square-OpenAPI-Key": SQ_KEY,
                                   "Content-Type": "application/json",
                                   "clienttype": "binanceSkill"})
        data = r.json()
        ok = str(data.get("code")) == "000000"
        if not ok: print("square error", data)
        return ok
    except Exception as e:
        print("square exception", e)
        return False


def gap():
    time.sleep(random.uniform(*GAP))


def main():
    state = load_state()
    now = time.time()
    signals = [a for a in scan() if now - state.get(a["symbol"], 0) > COOLDOWN_H * 3600]
    print(f"{len(signals)} new signal(s)")
    for i, a in enumerate(signals[:MAX_SIGNALS]):
        if i > 0:
            gap()
        send_telegram(telegram_text(a))
        gap()
        send_square(square_text(a))
        state[a["symbol"]] = time.time()
        save_state(state)
    save_state(state)


if __name__ == "__main__":
    main()
