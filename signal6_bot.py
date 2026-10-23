"""
6-Layer Signal Bot (crypto pump-setup scanner) - Telegram signals + Binance Square posts

Free public data only (no paid API, no Binance key needed for data):
  CoinGecko (market, trending, sectors), DefiLlama (stablecoins, TVL), Binance public candles
  with automatic OKX fallback (if Binance blocks the server), OKX funding rate / open interest,
  alternative.me Fear & Greed.

Each coin is checked through 6 layers:
  1 Market regime   2 Narrative   3 Fundamentals   4 Chart (1d/4h/1h/30m/15m)
  5 Flow (volume, taker buy, funding, OI)   6 Trigger + risk plan (entry/stop/target, R:R)

Score rules:  >= PASS_SCORE (40) coin goes on the list | >= SIGNAL_MIN (45) Telegram signal + Square post
NOT a guarantee of a pump. Probability checklist only.

Run:
  python signal6_bot.py --test-telegram
  python signal6_bot.py --tick [--force]
  python signal6_bot.py --once
"""
import json
import math
import os
import re
import sys
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

load_dotenv()

# ---------------- Settings (from .env / environment) ----------------

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")
SCAN_EVERY_MIN = int(os.getenv("SCAN_EVERY_MIN", "30"))
TICK_SECONDS = int(os.getenv("TICK_SECONDS", "60"))
TOP_N = int(os.getenv("TOP_N", "6"))
MIN_QV = float(os.getenv("MIN_QUOTE_VOLUME", "5000000"))      # min 24h USDT volume
STAGE1_COUNT = int(os.getenv("STAGE1_COUNT", "40"))            # coins pre-checked by volume
GAINERS_COUNT = int(os.getenv("GAINERS_COUNT", "15"))          # extra top gainers pre-checked
DEEP_COUNT = int(os.getenv("DEEP_COUNT", "12"))                # coins fully analyzed
MAX_CHANGE_24H = float(os.getenv("MAX_CHANGE_24H", "60"))
ALERT_MIN_SCORE = float(os.getenv("SIGNAL_MIN", "45"))
ALERT_JUMP = float(os.getenv("ALERT_SCORE_JUMP", "10"))
ALERT_COOLDOWN_H = float(os.getenv("ALERT_COOLDOWN_HOURS", "8"))
ALERT_STAGES = {x.strip().upper() for x in os.getenv("ALERT_STAGES", "EARLY,RUNNING").split(",") if x.strip()}
ENABLE_LISTING = os.getenv("ENABLE_NEW_LISTING_ALERTS", "true").lower() == "true"
DIGEST_EVERY_MIN = int(os.getenv("DIGEST_EVERY_MIN", "60"))      # send passed coins every N minutes
DIGEST_MIN_SCORE = float(os.getenv("DIGEST_MIN_SCORE", "50"))
DIGEST_SEND_EMPTY = os.getenv("DIGEST_SEND_EMPTY", "true").lower() == "true"
DIGEST_ONLY_NEW = os.getenv("DIGEST_ONLY_NEW", "false").lower() == "true"   # report only coins not reported recently
STATE_FILE = os.getenv("STATE_FILE", "state_signal6.json")

# Binance Square auto-post (optional). Needs a Square OpenAPI key (NOT your trading API key).
POST_SQUARE = os.getenv("POST_TO_SQUARE", "false").lower() == "true"
SQUARE_KEY = os.getenv("BINANCE_SQUARE_OPENAPI_KEY", "")
SQUARE_MIN_SCORE = float(os.getenv("SQUARE_MIN_SCORE", "45"))
SQUARE_EVERY_H = float(os.getenv("SQUARE_EVERY_HOURS", "3"))
SQUARE_URL = "https://www.binance.com/bapi/composite/v1/public/pgc/openApi/content/add"

# Score weights - tune these after /stats shows which signals work.
W = {
    "vol_spike": 25, "taker_buy": 15, "price_ok": 10, "rs_btc": 5,
    "rsi": 10, "macd": 8, "trend_1h": 7, "breakout": 5,
    "trend_4h": 8, "trend_15m": 5, "squeeze": 3, "bb_expand": 4,
    "sr_room": 5, "trending": 5,
}




BINANCE = ["https://data-api.binance.vision", "https://api.binance.com"]


STABLES = {"USDT", "USDC", "FDUSD", "TUSD", "DAI", "BUSD", "USDP", "USDE", "EUR", "TRY", "BRL", "WBTC", "WBETH"}


S = requests.Session()
S.headers.update({"User-Agent": "Mozilla/5.0 (binance-momentum-bot)"})
LAST_ERROR = [""]   # last failed HTTP request (for diagnostics)
SCAN_STATS = {}      # filled by scan(): universe / analyzed / best score / error


CHECKS = (("1h", 3600), ("6h", 21600), ("24h", 86400))


DISCLAIMER = ("⚠️ Not financial advice. The score reflects momentum and a risk filter only, "
              "not a guarantee of a pump. Always DYOR.")


def log(msg):
    msg = str(msg)
    for secret in (TG_TOKEN, SQUARE_KEY):
        if secret:
            msg = msg.replace(secret, "***")
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", flush=True)


def get_json(url, params=None, retries=2, timeout=20):
    for i in range(retries + 1):
        try:
            r = S.get(url, params=params, timeout=timeout)
            if r.status_code == 429:
                time.sleep(5 * (i + 1))
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa
            if i == retries:
                err = f"{url.split('?')[0].split('//')[-1].split('/')[0]}: {str(e)[:90]}"
                for secret in (TG_TOKEN, SQUARE_KEY):
                    if secret:
                        err = err.replace(secret, "***")
                LAST_ERROR[0] = err
                log(f"request failed: {url.split('?')[0]} -> {e}")
                return None
            time.sleep(2)
    return None


def _f(x, default=0.0):
    try:
        return float(x)
    except Exception:  # noqa
        return default


def usd(x):
    x = x or 0
    for div, suf in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(x) >= div:
            return f"${x / div:.2f}{suf}"
    return f"${x:.2f}"


def load_state():
    try:
        with open(STATE_FILE) as f:
            st = json.load(f)
    except Exception:  # noqa
        st = {}
    for k, v in (("tg_offset", 0), ("last_scan", 0), ("alerted", {}), ("picks", []), ("closed", []),
                 ("oi", {}), ("last_summary", 0), ("n_scans", 0), ("n_alerts", 0), ("last_tweet", 0)):
        st.setdefault(k, v)
    return st


def save_state(st):
    now = time.time()
    st["alerted"] = {k: v for k, v in st["alerted"].items() if now - v["ts"] < 3 * 86400}
    st["closed"] = st["closed"][-500:]
    st["picks"] = st["picks"][-200:]
    with open(STATE_FILE, "w") as f:
        json.dump(st, f)


def send_telegram(text):
    if not TG_TOKEN or not TG_CHAT:
        log("No Telegram token/chat id set, printing to console:")
        print(text)
        return False
    ok = True
    for i in range(0, len(text), 4000):
        try:
            r = S.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                       data={"chat_id": TG_CHAT, "text": text[i:i + 4000], "disable_web_page_preview": True},
                       timeout=20)
            if not r.ok:
                log(f"Telegram error: {r.status_code} {r.text[:200]}")
                ok = False
        except Exception as e:  # noqa
            log(f"Telegram error: {e}")
            ok = False
    return ok


def send_blocks(blocks):
    msg = ""
    for b in blocks:
        if msg and len(msg) + len(b) + 2 > 3900:
            send_telegram(msg)
            msg = ""
        msg += ("\n\n" if msg else "") + b
    if msg:
        send_telegram(msg)


def tg_updates(offset):
    if not TG_TOKEN:
        return []
    d = get_json(f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates",
                 params={"offset": offset, "timeout": 0, "allowed_updates": json.dumps(["message"])})
    return d.get("result", []) if isinstance(d, dict) else []


def handle_commands(st):
    cmds = {"scan": False, "coins": [], "stats": False, "help": False, "top100": False}
    for u in tg_updates(st["tg_offset"]):
        st["tg_offset"] = u["update_id"] + 1
        m = u.get("message") or {}
        if str((m.get("chat") or {}).get("id")) != str(TG_CHAT):
            continue  # ignore everyone except the owner
        text = (m.get("text") or "").strip()
        if not text.startswith("/"):
            continue
        parts = text.split()
        cmd = parts[0].split("@")[0].lower()
        if cmd == "/scan":
            cmds["scan"] = True
        elif cmd == "/coin" and len(parts) > 1:
            cmds["coins"].append(parts[1])
        elif cmd == "/stats":
            cmds["stats"] = True
        elif cmd == "/top100":
            cmds["top100"] = True
        elif cmd in ("/help", "/start"):
            cmds["help"] = True
    return cmds


def col(rows, i):
    return [r[i] for r in rows]


def ewm(values, alpha):
    out = [values[0]]
    for v in values[1:]:
        out.append(alpha * v + (1 - alpha) * out[-1])
    return out


def ema(values, n):
    return ewm(values, 2 / (n + 1))


def rsi(values, n=14):
    gains, losses = [0.0], [0.0]
    for a, b in zip(values, values[1:]):
        d = b - a
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    up, dn = ewm(gains, 1 / n)[-1], ewm(losses, 1 / n)[-1]
    return 100 - 100 / (1 + up / (dn if dn else 1e-12))


def macd_hist(values):
    m = [a - b for a, b in zip(ema(values, 12), ema(values, 26))]
    return [x - y for x, y in zip(m, ema(m, 9))]


def bollinger(c, n=20, k=2):
    out = []
    for i in range(n - 1, len(c)):
        w = c[i - n + 1:i + 1]
        m = sum(w) / n
        sd = math.sqrt(sum((x - m) ** 2 for x in w) / n)
        out.append((m, m + k * sd, m - k * sd, (2 * k * sd) / m if m else 0))
    return out


def squeeze_info(c):
    """(in_squeeze_now, broke_up_after_squeeze)"""
    bb = bollinger(c)
    if len(bb) < 30:
        return False, False
    bws = [x[3] for x in bb][-100:]
    thr = sorted(bws)[int(len(bws) * 0.2)]
    now_sq = bws[-1] <= thr
    recent_sq = any(b <= thr for b in bws[-7:-1])
    return now_sq, (c[-1] > bb[-1][1] and recent_sq)


def sr_levels(h, l, c, w=3):
    """nearest swing support below and resistance above the current price"""
    price = c[-1]
    res = [h[i] for i in range(w, len(h) - w) if h[i] == max(h[i - w:i + w + 1]) and h[i] > price]
    sup = [l[i] for i in range(w, len(l) - w) if l[i] == min(l[i - w:i + w + 1]) and l[i] < price]
    return (max(sup) if sup else None), (min(res) if res else None)


def to_4h(rows):
    out = {}
    for r in rows:
        k = int(r[0]) // 14400
        if k not in out:
            out[k] = [r[0], r[1], r[2], r[3], r[4], r[5]]
        else:
            b = out[k]
            b[2], b[3], b[4], b[5] = max(b[2], r[2]), min(b[3], r[3]), r[4], b[5] + r[5]
    return [out[k] for k in sorted(out)]


def entry_stage(c24, ratio, info, late_c24=30, early_c24=12):
    """EARLY = volume accelerating but price has barely moved. LATE = the pump already happened."""
    r = info.get("rsi", 50)
    if info.get("at_res") or info.get("rejected") or info.get("extended") or c24 > late_c24 or r > 75:
        return "LATE"
    if c24 <= early_c24 and ratio >= 2.5 and r < 66:
        return "EARLY"
    return "RUNNING"


def resistance_state(hi, c):
    """(at_resistance_level, rejected_level): price sitting under a level it touched recently,
    or a recent wick that hit a level and closed well below it."""
    n, price = len(c), c[-1]
    # only levels that formed BEFORE the last 6 candles (a running high in an uptrend is not resistance)
    levels = [max(hi[-30:-6])]
    if n >= 80:
        levels.append(max(hi[-78:-6]))
    w = 2
    levels += [hi[i] for i in range(w, n - 6) if hi[i] == max(hi[i - w:i + w + 1])]
    recent_hi = max(hi[-6:])
    at_res = rejected = None
    for L in sorted(set(levels)):
        if L <= 0:
            continue
        confirmed_break = price > L and c[-2] > L
        if 0.975 * L <= price <= 1.005 * L and recent_hi >= 0.995 * L and not confirmed_break:
            at_res = L
        for k in (1, 2, 3):
            if hi[-k] >= 0.995 * L and c[-k] <= 0.985 * L:
                rejected = L
    return at_res, rejected


def tech_analysis(h1, h4, m15):
    """returns (score 0-40, notes, warns, tags, info)"""
    s, notes, warns, tags, info = 0.0, [], [], [], {}
    if h1 and len(h1) >= 50:
        c, hi, lo = col(h1, 4), col(h1, 2), col(h1, 3)
        r = rsi(c)
        info["rsi"] = r
        if 50 <= r <= 68:
            s += W["rsi"]; tags.append("rsi_ok"); notes.append(f"1h RSI {r:.0f} (healthy momentum)")
        elif 40 <= r < 50:
            s += 4
        elif r > 75:
            s -= 6; warns.append(f"1h RSI {r:.0f} (overbought)")
        h = macd_hist(c)
        if h[-1] > 0 and h[-1] > h[-2]:
            s += W["macd"]; info["macd"] = "up"; tags.append("macd_up"); notes.append("1h MACD bullish and rising")
        elif h[-1] > 0:
            s += W["macd"] / 2; info["macd"] = "+"
        else:
            info["macd"] = "down"
        e20, e50 = ema(c, 20)[-1], ema(c, 50)[-1]
        if c[-1] > e20 > e50:
            s += W["trend_1h"]; tags.append("trend_1h"); notes.append("1h: price > EMA20 > EMA50")
        if c[-1] > max(hi[-25:-1]):
            s += W["breakout"]; info["breakout"] = True; tags.append("breakout"); notes.append("Broke above 24h high")
        sq, bu = squeeze_info(c)
        if bu:
            s += W["bb_expand"]; tags.append("bb_breakout"); notes.append("Bollinger breakout after a squeeze")
        elif sq:
            s += W["squeeze"]; info["squeeze"] = True; tags.append("squeeze"); notes.append("Bollinger squeeze (volatility compressed)")
        sup, res = sr_levels(hi, lo, c)
        price = c[-1]
        info["sup"], info["res"], info["price"] = sup, res, price
        at_res, rejected = resistance_state(hi, c)
        if at_res:
            s -= 10; info["at_res"] = True
            warns.append(f"At/near resistance {at_res:.6g} (touched recently) - poor entry")
        if rejected:
            s -= 8; info["rejected"] = True
            warns.append(f"Rejected at resistance {rejected:.6g} (wick back down)")
        if price / e20 - 1 > 0.10:
            s -= 6; info["extended"] = True
            warns.append(f"Extended: {(price / e20 - 1) * 100:.0f}% above 1h EMA20 (chasing risk)")
        if res and (res - price) / price < 0.02:
            s -= 5; warns.append(f"Resistance very close (+{(res - price) / price * 100:.1f}%)")
        elif (res is None or (res - price) / price > 0.08) and not (at_res or rejected):
            s += W["sr_room"]; tags.append("room_to_run"); notes.append("Room to run (no resistance within 8%)")
    if h4 and len(h4) >= 25:
        c4 = col(h4, 4)
        e = ema(c4, 20)
        if c4[-1] > e[-1] and e[-1] > e[-3]:
            s += W["trend_4h"]; info["t4"] = "up"; tags.append("trend_4h"); notes.append("4h trend up (above rising EMA20)")
        elif c4[-1] > e[-1]:
            s += 4; info["t4"] = "+"
        else:
            info["t4"] = "down"
    if m15 and len(m15) >= 40:
        c15 = col(m15, 4)
        e = ema(c15, 20)[-1]
        if c15[-1] > e and macd_hist(c15)[-1] > 0:
            s += W["trend_15m"]; info["t15"] = "up"; tags.append("trend_15m")
        else:
            info["t15"] = "down"
        if rsi(c15) > 80:
            s -= 3; warns.append("15m RSI very high (short-term overheated)")
    return max(0.0, min(s, 40.0)), notes, warns, tags, info


def tech_line(info):
    parts = []
    if "rsi" in info:
        parts.append(f"RSI {info['rsi']:.0f}")
    if "macd" in info:
        parts.append(f"MACD {info['macd']}")
    if "t4" in info:
        parts.append(f"4h {info['t4']}")
    if "t15" in info:
        parts.append(f"15m {info['t15']}")
    if info.get("squeeze"):
        parts.append("Squeeze")
    if info.get("breakout"):
        parts.append("Breakout")
    return " | ".join(parts)


def sr_line(info):
    price, sup, res = info.get("price"), info.get("sup"), info.get("res")
    if not price or not (sup or res):
        return ""
    a = f"Support {sup:.6g} ({(sup - price) / price * 100:+.1f}%)" if sup else ""
    b = f"Resistance {res:.6g} ({(res - price) / price * 100:+.1f}%)" if res else ""
    return " | ".join(x for x in (a, b) if x)


def binance_klines(sym, interval, limit):
    for base in BINANCE:
        d = get_json(f"{base}/api/v3/klines", params={"symbol": sym, "interval": interval, "limit": limit}, retries=1)
        if isinstance(d, list) and d:
            return d
    return None


def btc_regime():
    closes = None
    kl = binance_klines("BTCUSDT", "4h", 100)
    if kl:
        closes = [_f(k[4]) for k in kl]
    else:
        d = get_json("https://www.okx.com/api/v5/market/candles",
                     params={"instId": "BTC-USDT", "bar": "4H", "limit": "100"}, retries=1)
        try:
            closes = [_f(r[4]) for r in reversed(d["data"])]
        except Exception:  # noqa
            closes = None
    if not closes or len(closes) < 60:
        return {"name": "UNKNOWN", "factor": 1.0, "note": "BTC regime: unavailable (no filter applied)"}
    e50, r = ema(closes, 50)[-1], rsi(closes)
    ch = (closes[-1] / closes[-7] - 1) * 100
    above = closes[-1] > e50
    if not above and (r < 45 or ch < -3):
        name, f = "BEAR", 0.8
    elif above and r >= 50:
        name, f = "BULL", 1.0
    else:
        name, f = "NEUTRAL", 0.92
    note = (f"BTC regime: {name} (scores x{f:.2f}) | 4h RSI {r:.0f}, 24h {ch:+.1f}%, "
            f"{'above' if above else 'below'} EMA50")
    return {"name": name, "factor": f, "note": note}


def coingecko_trending():
    d = get_json("https://api.coingecko.com/api/v3/search/trending", retries=1)
    syms, cats = set(), []
    try:
        for c in d.get("coins", []):
            syms.add(str(c["item"]["symbol"]).upper())
        for c in d.get("categories", [])[:4]:
            if c.get("name"):
                cats.append(c["name"])
    except Exception:  # noqa
        pass
    return syms, cats


def okx_funding_oi(base):
    inst = f"{base}-USDT-SWAP"
    fr = oi = None
    f = get_json("https://www.okx.com/api/v5/public/funding-rate", params={"instId": inst}, retries=0)
    try:
        fr = float(f["data"][0]["fundingRate"])
    except Exception:  # noqa
        pass
    o = get_json("https://www.okx.com/api/v5/public/open-interest",
                 params={"instType": "SWAP", "instId": inst}, retries=0)
    try:
        oi = float(o["data"][0]["oiUsd"])
    except Exception:  # noqa
        pass
    return fr, oi


def record_picks(st, items):
    now = time.time()
    recent = st["picks"] + st["closed"][-100:]
    for it in items:
        if it["score"] < ALERT_MIN_SCORE or not it["price"]:
            continue
        if any(p["key"] == it["key"] and now - p["ts"] < 6 * 3600 for p in recent):
            continue
        st["picks"].append({"key": it["key"], "kind": it["kind"], "symbol": it["symbol"], "chain": it["chain"],
                            "addr": it["addr"], "ts": now, "price0": it["price"], "score": round(it["score"], 1),
                            "tags": it["tags"], "r": {}})


def fetch_prices(picks):
    out, by = {}, {}
    for p in picks:
        if p["kind"] == "dex":
            by.setdefault(p["chain"], []).append(p["addr"])
    for ch, addrs in by.items():
        addrs = list(dict.fromkeys(addrs))
        for i in range(0, len(addrs), 30):
            data = get_json(f"https://api.dexscreener.com/tokens/v1/{ch}/{','.join(addrs[i:i + 30])}")
            best = {}
            for pr in (data if isinstance(data, list) else []):
                try:
                    a = pr["baseToken"]["address"].lower()
                    liq = (pr.get("liquidity") or {}).get("usd") or 0
                    if a not in best or liq > best[a][1]:
                        best[a] = (_f(pr.get("priceUsd")), liq)
                except KeyError:
                    continue
            for a, (px, _) in best.items():
                out[f"{ch}:{a}"] = px
    if any(p["kind"] == "cex" for p in picks):
        for base in BINANCE:
            d = get_json(f"{base}/api/v3/ticker/price", retries=1)
            if isinstance(d, list):
                for t in d:
                    if str(t.get("symbol", "")).endswith("USDT"):
                        out[f"cex:{t['symbol'][:-4]}"] = _f(t.get("price"))
                break
    return out


def update_perf(st):
    now = time.time()
    due = [p for p in st["picks"] if any(k not in p["r"] and now - p["ts"] >= s for k, s in CHECKS)]
    if due:
        prices = fetch_prices(due)
        for p in due:
            px = prices.get(p["key"])
            if not px or not p["price0"]:
                continue
            for k, s in CHECKS:
                if k not in p["r"] and now - p["ts"] >= s:
                    p["r"][k] = round((px / p["price0"] - 1) * 100, 2)
    keep = []
    for p in st["picks"]:
        if "24h" in p["r"] or now - p["ts"] > 36 * 3600:
            st["closed"].append(p)
        else:
            keep.append(p)
    st["picks"] = keep


def perf_report(st):
    picks = st["closed"] + st["picks"]
    if not picks:
        return ("📈 Performance: no tracked picks yet. A pick is tracked when the bot sends an alert "
                "or report with score >= " + f"{ALERT_MIN_SCORE:.0f}.")
    lines = [f"📈 Performance ({len(picks)} picks tracked)"]
    for h, _ in CHECKS:
        v = sorted(p["r"][h] for p in picks if h in p["r"])
        if v:
            win = sum(1 for x in v if x > 0) / len(v) * 100
            lines.append(f"{h}: avg {sum(v) / len(v):+.1f}% | median {v[len(v) // 2]:+.1f}% | win {win:.0f}% (n={len(v)})")

    def ret(p):
        return p["r"].get("6h", p["r"].get("1h"))

    lines.append("\nBy score (6h return):")
    for name, lo, hi in (("70+", 70, 101), ("55-69", 55, 70), ("<55", 0, 55)):
        v = [ret(p) for p in picks if lo <= p["score"] < hi and ret(p) is not None]
        if v:
            lines.append(f"  {name}: avg {sum(v) / len(v):+.1f}% (n={len(v)})")
    by_tag = {}
    for p in picks:
        r = ret(p)
        if r is None:
            continue
        for t in p["tags"]:
            by_tag.setdefault(t, []).append(r)
    rows = sorted(((sum(v) / len(v), t, len(v)) for t, v in by_tag.items() if len(v) >= 3), reverse=True)
    if rows:
        lines.append("\nBest signals: " + ", ".join(f"{t} {a:+.1f}% (n={n})" for a, t, n in rows[:3]))
        lines.append("Worst signals: " + ", ".join(f"{t} {a:+.1f}% (n={n})" for a, t, n in rows[-3:]))
    if len(picks) < 30:
        lines.append("\n⚠️ Small sample (<30 picks): do not trust these numbers yet.")
    return "\n".join(lines)


def pick_alerts(st, items):
    now, out = time.time(), []
    for it in items:
        if it["score"] < ALERT_MIN_SCORE or it.get("stage", "RUNNING") not in ALERT_STAGES:
            continue
        a = st["alerted"].get(it["key"])
        if a is None or now - a["ts"] > ALERT_COOLDOWN_H * 3600:
            out.append((it, "🆕 NEW"))
        elif it["score"] >= a["score"] + ALERT_JUMP:
            out.append((it, f"📈 SCORE UP (+{it['score'] - a['score']:.0f})"))
    return out


def mark_alerted(st, items):
    now = time.time()
    for it in items:
        st["alerted"][it["key"]] = {"ts": now, "score": it["score"]}


def fmt_item(it, tag=""):
    lines = [f"{it['title']} - Score {it['score']:.0f}/100 {tag}".strip()]
    lines += it["lines"]
    lines += [f"✅ {n}" for n in it["notes"][:5]]
    lines += [f"ℹ️ {n}" for n in it["infos"][:3]]
    lines += [f"⚠️ {n}" for n in it["flags"][:6]]
    if it.get("url"):
        lines.append(it["url"])
    return "\n".join(lines)


HELP = ("🤖 Binance Momentum Bot - Commands\n"
        "/scan - run a full scan now\n"
        "/coin <SYMBOL> - analyze any Binance coin (e.g. /coin SOL)\n"
        "/top100 - analysis of the 100 biggest Binance coins\n"
        "/stats - performance of past picks\n"
        "/help - this message\n\n"
        "Note: commands are answered at the next bot cycle (up to ~15 min on GitHub Actions).")



NARRATIVES = {
    "AI": ["ai", "gpt", "agent", "agents", "neural", "llm", "bot"],
    "Meme-Dog": ["dog", "doge", "inu", "shib", "wif", "bonk", "puppy"],
    "Meme-Cat": ["cat", "kitty", "meow", "popcat", "mog"],
    "Meme-Frog": ["pepe", "frog", "kek"],
    "Politics": ["trump", "maga", "biden", "elon", "musk"],
    "Gaming": ["game", "gaming", "play", "arcade"],
    "RWA": ["rwa", "gold", "treasury"],
}


def narrative_tags(name, symbol):
    toks = re.findall(r"[a-z0-9]+", f"{name} {symbol}".lower())
    out = []
    for theme, words in NARRATIVES.items():
        if any(t == w or (len(w) >= 4 and w in t) for t in toks for w in words):
            out.append(theme)
    return out



# =====================================================================
#  6-LAYER ENGINE
#  L1 Market regime | L2 Narrative | L3 Fundamentals | L4 Chart structure
#  L5 Flow (volume/buy pressure/funding/OI) | L6 Trigger + risk plan
# =====================================================================
LAYER_NAMES = ["Market", "Narrative", "Fundamentals", "Chart", "Flow", "Trigger/Risk"]
LAYER_W = [15, 15, 15, 25, 20, 10]          # sums to 100
PASS_SCORE = float(os.getenv("PASS_SCORE", "40"))      # coin goes on the list
SIGNAL_MIN = float(os.getenv("SIGNAL_MIN", "45"))      # Telegram signal + Square post
MIN_RR = float(os.getenv("MIN_RR", "1.5"))
MIN_UPSIDE = float(os.getenv("MIN_UPSIDE", "3"))   # min % distance to target
MIN_LAYERS_OK = int(os.getenv("MIN_LAYERS_OK", "3"))   # layers that must score >= 50%
MIN_MCAP = float(os.getenv("MIN_MCAP", "20000000"))
MIN_VOL24 = float(os.getenv("MIN_VOL24", "8000000"))
DEEP_N = int(os.getenv("DEEP_N", "15"))
QUICK_N = int(os.getenv("QUICK_N", "60"))
BLOCKED = {"binance": False}
OKX_BAR = {"15m": "15m", "30m": "30m", "1h": "1H", "4h": "4H", "1d": "1D"}
BN_BAR = {"15m": "15m", "30m": "30m", "1h": "1h", "4h": "4h", "1d": "1d"}


def clamp(x, lo=0.0, hi=1.0):
    return max(lo, min(hi, x))


# ---------------- candles: Binance first, OKX fallback ----------------
def candles(base, tf, limit=200):
    """rows: [ts_sec, open, high, low, close, quote_volume, taker_buy_quote_or_0]"""
    sym = f"{base}USDT"
    if not BLOCKED["binance"]:
        kl = binance_klines(sym, BN_BAR[tf], limit)
        if kl:
            return [[_f(k[0]) / 1000, _f(k[1]), _f(k[2]), _f(k[3]), _f(k[4]), _f(k[7]), _f(k[10])] for k in kl]
        if kl is None and ("451" in LAST_ERROR[0] or "403" in LAST_ERROR[0]):
            BLOCKED["binance"] = True
            log("Binance looks blocked from this server, using OKX only")
    d = get_json("https://www.okx.com/api/v5/market/candles",
                 params={"instId": f"{base}-USDT", "bar": OKX_BAR[tf], "limit": str(min(limit, 300))}, retries=1)
    try:
        rows = [[_f(r[0]) / 1000, _f(r[1]), _f(r[2]), _f(r[3]), _f(r[4]), _f(r[7]), 0.0] for r in reversed(d["data"])]
    except Exception:  # noqa
        return None
    return rows if len(rows) >= (20 if tf == "1d" else 30) else None


def atr(rows, n=14):
    trs = [max(r[2] - r[3], abs(r[2] - p[4]), abs(r[3] - p[4])) for p, r in zip(rows, rows[1:])]
    return sum(trs[-n:]) / n if len(trs) >= n else 0.0


# ---------------- universe ----------------
def get_universe():
    """list of dicts: base, name, price, mcap, fdv, circ, total, vol24, c24, c7, gecko_id"""
    d = get_json("https://api.coingecko.com/api/v3/coins/markets",
                 params={"vs_currency": "usd", "order": "volume_desc", "per_page": 250, "page": 1,
                         "price_change_percentage": "24h,7d"})
    out = []
    if isinstance(d, list) and d:
        for c in d:
            base = str(c.get("symbol", "")).upper()
            if base in STABLES or not re.fullmatch(r"[A-Z0-9]{2,15}", base):
                continue
            out.append({"base": base, "name": c.get("name", ""), "gecko_id": c.get("id"), "price": _f(c.get("current_price")),
                        "mcap": _f(c.get("market_cap")), "fdv": _f(c.get("fully_diluted_valuation")),
                        "circ": _f(c.get("circulating_supply")), "total": _f(c.get("total_supply")),
                        "vol24": _f(c.get("total_volume")), "c24": _f(c.get("price_change_percentage_24h")),
                        "c7": _f(c.get("price_change_percentage_7d_in_currency")), "rank": c.get("market_cap_rank")})
        return out
    # fallback: OKX tickers (no fundamentals)
    t = get_json("https://www.okx.com/api/v5/market/tickers", params={"instType": "SPOT"})
    try:
        for x in t["data"]:
            if not x["instId"].endswith("-USDT"):
                continue
            base = x["instId"][:-5]
            if base in STABLES or not re.fullmatch(r"[A-Z0-9]{2,15}", base):
                continue
            last, op = _f(x["last"]), _f(x["open24h"])
            out.append({"base": base, "name": base, "gecko_id": None, "price": last, "mcap": 0, "fdv": 0, "circ": 0,
                        "total": 0, "vol24": _f(x["volCcy24h"]) * last, "c24": (last / op - 1) * 100 if op else 0,
                        "c7": None, "rank": None})
        out.sort(key=lambda c: c["vol24"], reverse=True)
    except Exception:  # noqa
        pass
    return out[:250]


# ---------------- context shared by all coins ----------------
def stable_trend():
    d = get_json("https://stablecoins.llama.fi/stablecoincharts/all")
    try:
        v = [_f((x.get("totalCirculatingUSD") or {}).get("peggedUSD")) for x in d[-15:]]
        return (v[-1] / v[-8] - 1) * 100 if v[-8] else None
    except Exception:  # noqa
        return None


def llama_protocols():
    d = get_json("https://api.llama.fi/protocols")
    out = {}
    if isinstance(d, list):
        for p in d:
            gid = p.get("gecko_id")
            if gid and p.get("tvl"):
                out[gid] = {"tvl": _f(p.get("tvl")), "d7": p.get("change_7d"), "d1": p.get("change_1d"), "cat": p.get("category")}
    return out


def hot_categories():
    d = get_json("https://api.coingecko.com/api/v3/coins/categories")
    try:
        rows = [c for c in d if c.get("market_cap_change_24h") is not None and _f(c.get("market_cap")) > 5e7]
        rows.sort(key=lambda c: _f(c["market_cap_change_24h"]), reverse=True)
        return [(c["name"], _f(c["market_cap_change_24h"])) for c in rows[:10]]
    except Exception:  # noqa
        return []


def fear_greed():
    d = get_json("https://api.alternative.me/fng/", params={"limit": 1})
    try:
        return int(d["data"][0]["value"])
    except Exception:  # noqa
        return None


def btc_dominance():
    d = get_json("https://api.coingecko.com/api/v3/global")
    try:
        return float(d["data"]["market_cap_percentage"]["btc"])
    except Exception:  # noqa
        return None


def build_context():
    ctx = {}
    btc = candles("BTC", "4h", 120)
    if btc and len(btc) >= 60:
        c = col(btc, 4)
        e50, r = ema(c, 50)[-1], rsi(c)
        ch = (c[-1] / c[-7] - 1) * 100
        above = c[-1] > e50
        if above and r >= 50:
            name, s = "BULL", 1.0
        elif not above and (r < 45 or ch < -3):
            name, s = "BEAR", 0.15
        else:
            name, s = "NEUTRAL", 0.55
        ctx["btc"] = {"name": name, "s": s, "rsi": r, "ch24": ch, "above": above}
    else:
        ctx["btc"] = {"name": "UNKNOWN", "s": 0.5, "rsi": 50, "ch24": 0, "above": None}
    ctx["stable7"] = stable_trend()
    ctx["fng"] = fear_greed()
    ctx["btc_dom"] = btc_dominance()
    ctx["trend_syms"], ctx["trend_cats"] = coingecko_trending()
    ctx["hot_cats"] = hot_categories()
    ctx["llama"] = llama_protocols()
    b = ctx["btc"]
    ctx["line"] = (f"BTC: {b['name']} (4h RSI {b['rsi']:.0f}, 24h {b['ch24']:+.1f}%, "
                   f"{'above' if b['above'] else 'below'} EMA50)" +
                   (f" | Stablecoin supply 7d {ctx['stable7']:+.2f}%" if ctx["stable7"] is not None else "") +
                   (f" | Fear&Greed {ctx['fng']}" if ctx.get("fng") is not None else "") +
                   (f" | BTC dom {ctx['btc_dom']:.1f}%" if ctx.get("btc_dom") else ""))
    return ctx


# ---------------- the 6 layers ----------------
def layer1_market(ctx):
    b, s7 = ctx["btc"], ctx["stable7"]
    s = b["s"] * 0.75
    notes, flags = [], []
    st = 0.5 if s7 is None else clamp(0.5 + s7 / 2.0)     # +1% weekly growth -> 1.0
    s += st * 0.25
    if b["name"] == "BULL":
        notes.append("BTC in uptrend (above 4h EMA50, RSI >= 50): altcoins have tailwind")
    elif b["name"] == "BEAR":
        flags.append("BTC weak (below EMA50): most altcoin pumps fail in this regime")
    fng = ctx.get("fng")
    if fng is not None:
        if fng >= 80:
            s -= 0.1
            flags.append(f"Fear & Greed {fng}: extreme greed, chasing is risky")
        elif fng <= 25:
            s += 0.03
            notes.append(f"Fear & Greed {fng}: extreme fear (contrarian dip-buy zone)")
    if s7 is not None and s7 > 0.3:
        notes.append(f"Stablecoin supply growing {s7:+.2f}% (7d): fresh liquidity")
    elif s7 is not None and s7 < -0.3:
        flags.append(f"Stablecoin supply shrinking {s7:+.2f}% (7d)")
    return clamp(s), notes, flags


def layer2_narrative(c, ctx, detail):
    s, notes, flags = 0.2, [], []
    if c["base"] in ctx["trend_syms"]:
        s += 0.35
        notes.append("Trending on CoinGecko (attention rising)")
    cats = (detail or {}).get("categories") or []
    hot = {n for n, _ in ctx["hot_cats"]}
    hit = [x for x in cats if x in hot]
    if hit:
        chg = dict(ctx["hot_cats"]).get(hit[0], 0)
        s += 0.35
        notes.append(f"Sector '{hit[0]}' is hot today (sector mcap {chg:+.1f}% 24h)")
    themes = narrative_tags(c["name"], c["base"])
    if themes:
        s += 0.1
        notes.append("Theme: " + ", ".join(themes))
    sent = (detail or {}).get("sentiment_votes_up_percentage")
    if sent is not None and sent >= 70:
        s += 0.1
        notes.append(f"CoinGecko community sentiment {sent:.0f}% bullish")
    if cats and not hit and c["base"] not in ctx["trend_syms"]:
        flags.append("No hot sector/trending signal (no clear catalyst; check news manually)")
    if c["c7"] is not None and c["c7"] > 60:
        s -= 0.2
        flags.append(f"Already +{c['c7']:.0f}% in 7d: narrative may be priced in")
    return clamp(s), notes, flags


def layer3_fundamentals(c, ctx):
    notes, flags = [], []
    if not c["mcap"]:
        return 0.4, notes, ["Fundamental data unavailable (CoinGecko down)"]
    s = 0.3
    ratio = c["mcap"] / c["fdv"] if c["fdv"] else None
    if ratio is not None:
        if ratio >= 0.7:
            s += 0.25
            notes.append(f"Low unlock risk: {ratio * 100:.0f}% of fully-diluted supply already circulating")
        elif ratio >= 0.4:
            s += 0.1
        else:
            s -= 0.1
            flags.append(f"Only {ratio * 100:.0f}% of supply circulating: big future unlocks/dilution possible (check token unlock calendar)")
    turn = c["vol24"] / c["mcap"] if c["mcap"] else 0
    if 0.04 <= turn <= 0.6:
        s += 0.2
        notes.append(f"Healthy volume/market-cap ({turn * 100:.0f}%)")
    elif turn > 1.2:
        s -= 0.1
        flags.append(f"Volume is {turn:.1f}x market cap: speculative frenzy")
    if c["mcap"] >= 1e9:
        s += 0.05
    lp = ctx["llama"].get(c["gecko_id"])
    if lp:
        d7 = lp["d7"]
        notes.append(f"DefiLlama TVL ${lp['tvl'] / 1e6:.0f}M ({lp['cat']})" + (f", 7d {d7:+.1f}%" if d7 is not None else ""))
        if d7 is not None and d7 > 5:
            s += 0.2
            notes.append("TVL growing: real usage rising")
        elif d7 is not None and d7 < -10:
            s -= 0.1
            flags.append(f"TVL falling {d7:.0f}% (7d)")
        else:
            s += 0.05
    return clamp(s), notes, flags


def mtf_score(d1, m30, info):
    """higher timeframe sets direction (1d), 30m/15m time the entry"""
    s, notes, flags = 0.0, [], []
    if d1 and len(d1) >= 25:
        c = col(d1, 4)
        if c[-1] > ema(c, 20)[-1]:
            s += 0.4
            notes.append("Daily trend up (price above 1d EMA20)")
        else:
            flags.append("Daily trend down (below 1d EMA20): trading against the bigger trend")
    else:
        s += 0.15
    if m30 and len(m30) >= 30:
        c = col(m30, 4)
        r = rsi(c)
        if c[-1] > ema(c, 20)[-1] and 48 <= r <= 72:
            s += 0.35
            notes.append(f"30m trend up, RSI {r:.0f} (room to run)")
        elif r > 78:
            flags.append(f"30m RSI {r:.0f}: short-term overheated")
    else:
        s += 0.1
    if info.get("t15") == "up":
        s += 0.25
    return clamp(s), notes, flags


def layer4_chart(h1, h4, m15, d1=None, m30=None):
    ts, tn, tw, tt, info = tech_analysis(h1, h4, m15)
    ms, mn, mw = mtf_score(d1, m30, info)
    return clamp(ts / 34.0 * 0.75 + ms * 0.25), tn + mn, tw + mw, tt, info


def layer5_flow(c, h1, oi_state, base):
    notes, flags, tags = [], [], []
    s = 0.0
    vols = col(h1, 5)
    b = sum(vols[-26:-2]) / 24 if len(vols) >= 27 else 0
    ratio = vols[-2] / b if b else 0
    s += min(ratio, 3.5) / 3.5 * 0.4
    if ratio >= 2:
        tags.append("vol_spike")
        notes.append(f"Volume {ratio:.1f}x vs 24h average (last closed 1h)")
    tot = sum(vols[-4:-1])
    taker = sum(col(h1, 6)[-4:-1]) / tot if tot and sum(col(h1, 6)[-4:-1]) else None
    if taker is not None:
        s += clamp((taker - 0.45) / 0.15) * 0.25
        if taker >= 0.56:
            notes.append(f"Taker buy ratio {taker * 100:.0f}% (real buying pressure)")
    else:
        s += 0.1
    fr, oi = okx_funding_oi(base)
    fund = None
    if fr is not None:
        fund = f"Funding {fr * 100:+.3f}%"
        if fr > 0.0005:
            s -= 0.1
            flags.append(f"Funding {fr * 100:.3f}%: crowded longs")
        elif fr < 0:
            s += 0.1
            notes.append("Negative funding (shorts crowded: squeeze fuel)")
        else:
            s += 0.05
    if oi:
        prev = oi_state.get(base)
        if prev and prev.get("v"):
            chg = (oi / prev["v"] - 1) * 100
            if chg > 5 and c["c24"] > 0:
                s += 0.15
                notes.append(f"Open interest +{chg:.1f}% since last scan with rising price")
        oi_state[base] = {"v": oi, "ts": time.time()}
    if 1 <= c["c24"] <= 15:
        s += 0.1
    elif c["c24"] > 30:
        s -= 0.2
        flags.append(f"Already +{c['c24']:.0f}% in 24h")
    return clamp(s), notes, flags, ratio, fund


def layer6_trigger(c, h1, info, ratio, l4):
    """entry/stop/target plan + stage + hard vetoes"""
    price = h1[-1][4]
    a = atr(h1)
    sup, res = info.get("sup"), info.get("res")
    stop = price - 2 * a if a else price * 0.95
    if sup and 0.01 < (price - sup) / price < 0.08:
        stop = min(stop, sup * 0.99) if (price - sup * 0.99) / price < 0.09 else stop
    stop = min(stop, price * 0.985)
    risk = price - stop
    target = res if (res and (res - price) / price > 0.02) else price + 3 * a
    rr = (target - price) / risk if risk > 0 else 0
    stage = entry_stage(c["c24"], ratio, info)
    notes, flags, veto = [], [], []
    s = 0.0
    if rr >= 3:
        s += 0.5
    elif round(rr, 1) >= MIN_RR:
        s += 0.35
    else:
        flags.append(f"Risk:Reward only {rr:.1f} (need >= {MIN_RR:.1f}): not worth it")
        veto.append("low R:R")
    if stage == "EARLY":
        s += 0.4
    elif stage == "RUNNING":
        s += 0.25
    else:
        flags.append("Stage LATE: the move already happened (at resistance / extended / overbought)")
        veto.append("late")
    if info.get("t15") == "up":
        s += 0.1
    if info.get("at_res") or info.get("rejected"):
        veto.append("resistance")
    up = (target / price - 1) * 100
    if up < MIN_UPSIDE:
        flags.append(f"Only {up:+.1f}% room to the target (need >= {MIN_UPSIDE:.0f}%): profit too small")
        veto.append("small upside")
    notes.append(f"Plan: entry ~{price:.6g} | stop {stop:.6g} ({(stop / price - 1) * 100:.1f}%) | "
                 f"target {target:.6g} ({(target / price - 1) * 100:+.1f}%) | R:R {rr:.1f}")
    return clamp(s), notes, flags, veto, stage, {"price": price, "stop": stop, "target": target, "rr": rr}


# ---------------- evaluate one coin ----------------
def coin_detail(c):
    if not c["gecko_id"]:
        return None
    d = get_json(f"https://api.coingecko.com/api/v3/coins/{c['gecko_id']}",
                 params={"localization": "false", "tickers": "false", "market_data": "false", "community_data": "false",
                         "developer_data": "false", "sparkline": "false"}, retries=1)
    return d if isinstance(d, dict) else None


def evaluate(c, ctx, st, h1=None, with_detail=True):
    h1 = h1 or candles(c["base"], "1h", 200)
    if not h1 or len(h1) < 60:
        return None
    h4 = candles(c["base"], "4h", 100) or to_4h(h1)
    m15 = candles(c["base"], "15m", 100)
    d1 = candles(c["base"], "1d", 60)
    m30 = candles(c["base"], "30m", 100)
    detail = coin_detail(c) if with_detail else None
    time.sleep(1.5)
    l1, n1, f1 = layer1_market(ctx)
    l2, n2, f2 = layer2_narrative(c, ctx, detail)
    l3, n3, f3 = layer3_fundamentals(c, ctx)
    l4, n4, f4, tags, info = layer4_chart(h1, h4, m15, d1, m30)
    l5, n5, f5, ratio, fund = layer5_flow(c, h1, st["oi"], c["base"])
    l6, n6, f6, veto, stage, plan = layer6_trigger(c, h1, info, ratio, l4)
    ls = [l1, l2, l3, l4, l5, l6]
    total = sum(x * w for x, w in zip(ls, LAYER_W))
    ok_layers = sum(1 for x in ls if x >= 0.5)
    if ok_layers < MIN_LAYERS_OK:
        veto.append(f"only {ok_layers}/6 layers OK")
    if ctx["btc"]["name"] == "BEAR":
        veto.append("BTC weak")
    passed = total >= PASS_SCORE and not veto
    signal = passed and total >= SIGNAL_MIN
    return {"key": f"cex:{c['base']}", "kind": "cex", "symbol": c["base"], "chain": "cex", "addr": f"{c['base']}USDT",
            "title": f"{c['base']}/USDT", "score": total, "layers": ls, "ok": ok_layers, "veto": veto, "passed": passed, "signal": signal,
            "stage": stage, "plan": plan, "price": plan["price"], "c24": c["c24"], "c7": c["c7"], "mcap": c["mcap"],
            "vol24": c["vol24"], "notes": n2 + n3 + n4 + n5 + n1, "flags": f1 + f2 + f3 + f4 + f6 + f5,
            "plan_line": n6[0], "fund": fund, "tags": tags + [f"stage:{stage}"] + (["narrative"] if l2 >= 0.55 else []),
            "info": info}


# ---------------- scan ----------------
STATS = {}


def scan(ctx, st):
    STATS.clear()
    uni = get_universe()
    if not uni:
        STATS["error"] = f"Could not fetch market data (CoinGecko and OKX). Last error: {LAST_ERROR[0] or 'unknown'}"
        return []
    cand = [c for c in uni if c["vol24"] >= MIN_VOL24 and (not c["mcap"] or c["mcap"] >= MIN_MCAP) and -6 <= c["c24"] <= 35]
    log(f"universe {len(uni)} | candidates {len(cand)}")
    quick = []
    for c in cand[:QUICK_N]:
        h1 = candles(c["base"], "1h", 200)
        if not h1 or len(h1) < 60:
            continue
        try:
            ts, _, _, _, info = tech_analysis(h1, to_4h(h1), None)
        except Exception as e:  # noqa
            log(f"quick failed {c['base']}: {e}")
            continue
        bonus = 0.15 if info.get("at_res") or info.get("rejected") else 0
        vols = col(h1, 5)
        b = sum(vols[-26:-2]) / 24 if len(vols) >= 27 else 0
        ratio = vols[-2] / b if b else 0
        quick.append((ts / 34 + min(ratio, 3) / 3 - bonus, c, h1))
        time.sleep(0.12)
    quick.sort(key=lambda x: x[0], reverse=True)
    items = []
    for _, c, h1 in quick[:DEEP_N]:
        try:
            it = evaluate(c, ctx, st, h1)
        except Exception as e:  # noqa
            log(f"evaluate failed {c['base']}: {e}")
            it = None
        if it:
            items.append(it)
    items.sort(key=lambda x: (x["passed"], x["score"]), reverse=True)
    STATS.update({"universe": len(uni), "candidates": len(cand), "quick": len(quick), "deep": len(items),
                  "passed": sum(1 for i in items if i["passed"]), "best": max((i["score"] for i in items), default=0),
                  "binance_blocked": BLOCKED["binance"]})
    return items


# ---------------- formatting ----------------
def bar(x):
    n = int(round(x * 5))
    return "█" * n + "░" * (5 - n)


def fmt_item(it, tag=""):
    verdict = ("✅ SIGNAL" if it.get("signal") else "✅ PASSED (list)") if it["passed"] else "⛔ " + ("watch only: " + ", ".join(it["veto"]) if it["veto"] else "below pass score")
    mc = f" | MC {usd(it['mcap'])}" if it.get("mcap") else ""
    L = [f"{it['title']} [{it['stage']}] - {it['score']:.0f}/100 {tag}".strip(), verdict,
         f"Price {it['price']:.6g} | 24h {it['c24']:+.1f}%" + (f" | 7d {it['c7']:+.1f}%" if it.get("c7") is not None else "") + mc,
         "Layers: " + " ".join(f"{n[:4]} {bar(x)}" for n, x in zip(["Mkt", "Narr", "Fund", "Chart", "Flow", "Trig"], it["layers"])),
         it["plan_line"]]
    if it.get("fund"):
        L.append(it["fund"])
    L += [f"✅ {n}" for n in it["notes"][:6]]
    L += [f"⚠️ {n}" for n in it["flags"][:5]]
    L.append(f"https://www.tradingview.com/chart/?symbol=BINANCE%3A{it['symbol']}USDT")
    return "\n".join(L)


LEGEND = ("Layers: Mkt=market regime, Narr=narrative, Fund=fundamentals, Chart=structure/resistance, Flow=volume/funding/OI, "
          "Trig=entry+risk plan. Listed if score >= %.0f, signal if >= %.0f; also needs R:R >= %.1f, %d+ layers OK, not LATE, "
          "not at resistance, BTC not weak. News and token unlock dates are NOT checked automatically: verify before buying."
          % (PASS_SCORE, SIGNAL_MIN, MIN_RR, MIN_LAYERS_OK))
DISCLAIMER = ("⚠️ Not financial advice. This is a probability checklist, not a prediction. Many setups fail; "
              "risk only 1-2% of your capital per trade and always use the stop.")


def header(ctx, title="6-Layer Signal Scan"):
    now = datetime.now(timezone.utc).strftime("%d %b %Y %H:%M UTC")
    out = [f"🧭 {title} - {now}", ctx["line"]]
    if ctx["hot_cats"]:
        out.append("Hot sectors: " + ", ".join(f"{n} ({c:+.1f}%)" for n, c in ctx["hot_cats"][:4]))
    return "\n".join(out)


def stats_line():
    if STATS.get("error"):
        return "⚠️ " + STATS["error"]
    s = STATS
    return (f"🔧 {s['universe']} coins | {s['candidates']} candidates | {s['quick']} quick-scored | {s['deep']} 6-layer analyzed | "
            f"best {s['best']:.0f} | PASSED {s['passed']}" + (" | Binance blocked, using OKX" if s.get("binance_blocked") else ""))


def full_report(ctx, items):
    blocks = [header(ctx), stats_line()]
    shown = [i for i in items if i["passed"]] + [i for i in items if not i["passed"]][:3]
    if not any(i["passed"] for i in items):
        blocks.append("No coin passed all 6 layers right now. Waiting is a valid position. Closest watchlist below.")
    blocks += [f"{n}) " + fmt_item(it) for n, it in enumerate(shown, 1)]
    blocks += [LEGEND, DISCLAIMER]
    return blocks


def new_passed(st, items):
    now, out = time.time(), []
    for it in items:
        if not it.get("signal"):
            continue
        a = st["alerted"].get(it["key"])
        if a is None or now - a["ts"] > 12 * 3600:
            out.append((it, "🆕 NEW"))
        elif it["score"] >= a["score"] + 8:
            out.append((it, f"📈 SCORE UP (+{it['score'] - a['score']:.0f})"))
    return out


def fetch_prices(picks):
    out = {}
    t = get_json("https://www.okx.com/api/v5/market/tickers", params={"instType": "SPOT"})
    try:
        for x in t["data"]:
            if x["instId"].endswith("-USDT"):
                out[f"cex:{x['instId'][:-5]}"] = _f(x["last"])
    except Exception:  # noqa
        pass
    if not out:
        for host in BINANCE:
            d = get_json(f"{host}/api/v3/ticker/price", retries=1)
            if isinstance(d, list):
                out = {f"cex:{x['symbol'][:-4]}": _f(x["price"]) for x in d if str(x.get("symbol", "")).endswith("USDT")}
                break
    return out


def analyze_coin(q, ctx, st):
    base = re.sub(r"[^A-Za-z0-9]", "", q).upper().replace("USDT", "") or ""
    if not base:
        return "Usage: /coin <SYMBOL>  (example: /coin SOL)"
    uni = {c["base"]: c for c in get_universe()}
    c = uni.get(base) or {"base": base, "name": base, "gecko_id": None, "price": 0, "mcap": 0, "fdv": 0, "circ": 0,
                          "total": 0, "vol24": 0, "c24": 0, "c7": None, "rank": None}
    it = evaluate(c, ctx, st)
    if not it:
        return f"Not enough candle data for {base} on Binance/OKX spot."
    return header(ctx, "6-Layer analysis") + "\n\n" + fmt_item(it) + f"\n\n{LEGEND}\n\n{DISCLAIMER}"


# ---------------- Binance Square: natural-sounding posts ----------------
import random

OPENERS = [
    "Chart check done for this hour, a couple of setups are worth a look.",
    "Went through the top coins again, these ones look the cleanest right now.",
    "Not much is lining up today, but these stand out from the rest.",
    "Quick market scan finished. Here is what caught my eye.",
    "Scanning the majors again and a few names are starting to build momentum.",
]
CLOSERS = [
    "Just my notes, not financial advice. Do your own research and keep size small.",
    "Nothing here is a guarantee. DYOR and always respect your stop.",
    "Not advice, just what the data shows right now. Manage your risk.",
    "Setups can fail, so size small and use a stop. DYOR.",
]
COIN_T = [
    "${s} is around {p}. {why}. First target near {t}, idea is invalid below {st}.",
    "${s} ({p}): {why}. I would watch {t} as the first area, and {st} is where this setup breaks.",
    "Watching ${s} at {p}. {why}. Target zone {t}, stop zone {st} (R:R about {rr}).",
]


def short_why(it):
    for n in it["notes"]:
        if n.startswith(("Plan", "DefiLlama")):
            continue
        return n.split(" (")[0][:90].rstrip(". ")
    return "momentum is building"


def human_post(items, ctx):
    rnd = random.Random(int(time.time() // 600))
    parts = [rnd.choice(OPENERS), ""]
    for it in items:
        pl = it["plan"]
        parts.append(rnd.choice(COIN_T).format(s=it["symbol"], p=f"{pl['price']:.6g}", t=f"{pl['target']:.6g}",
                                               st=f"{pl['stop']:.6g}", rr=f"{pl['rr']:.1f}", why=short_why(it)))
    b = ctx["btc"]
    if rnd.random() < 0.6 and b["name"] != "UNKNOWN":
        parts.append("")
        parts.append(f"BTC is {'holding above' if b['above'] else 'below'} its 4h trend, so keep that in mind.")
    parts += ["", rnd.choice(CLOSERS)]
    return "\n".join(parts)[:1900]


def maybe_post_square(st, items, ctx):
    if not POST_SQUARE:
        return
    if not SQUARE_KEY:
        log("Square posting is ON but BINANCE_SQUARE_OPENAPI_KEY is missing")
        return
    now = time.time()
    if now - st.get("last_square", 0) < SQUARE_EVERY_H * 3600:
        return
    posted = st.setdefault("sq_posted", {})
    fresh = [i for i in items if i.get("signal") and i["score"] >= SQUARE_MIN_SCORE and now - posted.get(i["key"], 0) > 24 * 3600]
    fresh = sorted(fresh, key=lambda i: i["score"], reverse=True)[:2]
    if not fresh:
        return
    text = human_post(fresh, ctx)
    try:
        r = S.post(SQUARE_URL, json={"bodyTextOnly": text}, timeout=20,
                   headers={"X-Square-OpenAPI-Key": SQUARE_KEY, "Content-Type": "application/json", "clienttype": "binanceSkill"})
        d = r.json()
    except Exception as e:  # noqa
        log(f"Square post failed: {e}")
        send_telegram("⚠️ Binance Square post failed (network error).")
        return
    code = str(d.get("code"))
    if code in ("000000", "0") or d.get("success") is True:
        pid = (d.get("data") or {}).get("id") if isinstance(d.get("data"), dict) else None
        st["last_square"] = now
        for i in fresh:
            posted[i["key"]] = now
        send_telegram("✅ Posted to Binance Square: " + (f"https://www.binance.com/square/post/{pid}" if pid else "(link not returned)"))
    else:
        msg = f"Square post rejected: code {code} {str(d.get('message') or d.get('msg') or '')[:120]}"
        log(msg)
        send_telegram("⚠️ " + msg)


# ---------------- main cycle ----------------
HELP = ("🤖 6-Layer Signal Bot\n/scan - full scan now\n/coin <SYMBOL> - 6-layer analysis of one coin (e.g. /coin SOL)\n"
        "/stats - how past signals performed (1h/6h/24h)\n/help\n\nCommands are answered at the next hourly run.")


def tick(force=False):
    st = load_state()
    cache = []

    def ctx_():
        if not cache:
            cache.append(build_context())
        return cache[0]

    try:
        cmds = handle_commands(st)
        if cmds["help"]:
            send_telegram(HELP)
        update_perf(st)
        if cmds["stats"]:
            send_telegram(perf_report(st))
        for q in cmds["coins"][:3]:
            try:
                send_telegram(analyze_coin(q, ctx_(), st))
            except Exception as e:  # noqa
                log(f"/coin failed: {e}")
                send_telegram("Could not analyze that coin right now.")
        now = time.time()
        full = force or cmds["scan"]
        if full or now - st["last_scan"] >= SCAN_EVERY_MIN * 60 - 120:
            ctx = ctx_()
            items = scan(ctx, st)
            st["last_scan"], st["n_scans"] = now, st["n_scans"] + 1
            if full:
                send_blocks(full_report(ctx, items))
                passed = [i for i in items if i.get("signal")]
            else:
                alerts = new_passed(st, items)
                passed = [i for i, _ in alerts]
                if alerts:
                    send_blocks([header(ctx, "6-Layer SIGNAL"), stats_line()] + [fmt_item(i, t) for i, t in alerts] +
                                [LEGEND, DISCLAIMER])
                    st["n_alerts"] += len(alerts)
                elif STATS.get("error") and now - st.get("last_err", 0) > 6 * 3600:
                    send_telegram("⚠️ " + STATS["error"])
                    st["last_err"] = now
                else:
                    log("no new passed coins")
            sig = [i for i in items if i.get("signal")]
            maybe_post_square(st, sig, ctx)
            record_picks(st, [i for i in passed if i.get("signal")])
            mark_alerted(st, [i for i in passed if i.get("signal")])
        if st["last_summary"] == 0:
            st["last_summary"] = now
        elif now - st["last_summary"] >= 86400:
            send_blocks([f"🟢 Daily summary: {st['n_scans']} scans, {st['n_alerts']} signals in 24h.", perf_report(st)])
            st["n_scans"] = st["n_alerts"] = 0
            st["last_summary"] = now
    finally:
        save_state(st)


if __name__ == "__main__":
    if "--test-telegram" in sys.argv:
        print("OK" if send_telegram("✅ 6-Layer bot connected! Telegram is working.") else "FAILED - check token/chat id")
    elif "--tick" in sys.argv:
        tick(force="--force" in sys.argv)
    elif "--once" in sys.argv:
        tick(force=True)
    else:
        while True:
            try:
                tick()
            except Exception as e:  # noqa
                log(f"error: {e}")
            time.sleep(TICK_SECONDS)
