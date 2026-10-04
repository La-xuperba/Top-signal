"""
Crypto Momentum Scanner v2 (Solana + BNB DEX coins, plus large CEX coins)

Free public endpoints only (no paid API key):
  DexScreener, GeckoTerminal, DefiLlama, RugCheck, GoPlus, Honeypot.is,
  Binance public market data, OKX public data, CoinGecko trending.

Features
  1. Performance tracking of every pick (1h / 6h / 24h) -> /stats
  2. Alerts only for NEW coins or big score jumps (no repeats)
  3. Telegram commands: /scan  /coin <address>  /stats  /help
  4. Holder / dev wallet / LP-lock safety checks
  5. BTC market regime filter (scores are scaled down in a bear market)
  6. 15m + 1h + 4h analysis, Bollinger squeeze, support/resistance
  7. Large-cap CEX scan with funding rate + open interest
  8. CoinGecko trending + simple narrative tags

This is NOT a guarantee that any coin will pump. It is a momentum + risk score.

Run:
  python bot.py --test-telegram   # connection test
  python bot.py --once            # one forced scan + full report
  python bot.py --tick            # one cycle (used by GitHub Actions, every 15 min)
  python bot.py --tick --force    # same, but force a full scan
  python bot.py --watch 12        # keep running for 12 minutes (GitHub Actions near-real-time mode)
  python bot.py                   # loop forever (Termux / PC)
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
CHAINS = [c.strip() for c in os.getenv("CHAINS", "solana,bsc").split(",") if c.strip()]
SCAN_EVERY_MIN = int(os.getenv("SCAN_EVERY_MIN", "60"))
TICK_SECONDS = int(os.getenv("TICK_SECONDS", "60"))
TOP_N = int(os.getenv("TOP_N", "5"))
SHORTLIST = int(os.getenv("SHORTLIST", "10"))
MIN_LIQ = float(os.getenv("MIN_LIQUIDITY_USD", "30000"))
MIN_VOL = float(os.getenv("MIN_VOLUME_24H_USD", "50000"))
MIN_AGE_H = float(os.getenv("MIN_AGE_HOURS", "6"))
ALERT_MIN_SCORE = float(os.getenv("ALERT_MIN_SCORE", "55"))
ALERT_JUMP = float(os.getenv("ALERT_SCORE_JUMP", "10"))
ALERT_COOLDOWN_H = float(os.getenv("ALERT_COOLDOWN_HOURS", "12"))
ALERT_STAGES = {x.strip().upper() for x in os.getenv("ALERT_STAGES", "EARLY,RUNNING").split(",") if x.strip()}
DIGEST_EVERY_MIN = int(os.getenv("DIGEST_EVERY_MIN", "60"))      # send passed coins every N minutes
DIGEST_MIN_SCORE = float(os.getenv("DIGEST_MIN_SCORE", "50"))
DIGEST_SEND_EMPTY = os.getenv("DIGEST_SEND_EMPTY", "true").lower() == "true"
DIGEST_ONLY_NEW = os.getenv("DIGEST_ONLY_NEW", "false").lower() == "true"   # report only coins not reported recently
ENABLE_CEX = os.getenv("ENABLE_CEX", "true").lower() == "true"
CEX_MIN_QV = float(os.getenv("CEX_MIN_QUOTE_VOLUME", "20000000"))
CEX_N = int(os.getenv("CEX_SCAN_COUNT", "8"))
POST_TWITTER = os.getenv("POST_TO_TWITTER", "false").lower() == "true"
TWEET_EVERY_H = float(os.getenv("TWEET_EVERY_HOURS", "6"))
TWEET_MIN_SCORE = float(os.getenv("TWEET_MIN_SCORE", "65"))
STATE_FILE = os.getenv("STATE_FILE", "state.json")
GT_DELAY = 2.2  # GeckoTerminal free limit ~30 req/min

# Score weights - tune these after /stats shows which signals work.
W = {
    "vol_spike": 25, "buy_pressure": 15, "turnover": 10, "price_ok": 10,
    "rsi": 10, "macd": 8, "trend_1h": 7, "breakout": 5,
    "trend_4h": 8, "trend_15m": 5, "squeeze": 3, "bb_expand": 4,
    "sr_room": 5, "trending": 5,
}

LLAMA_NAMES = {"solana": "Solana", "bsc": "BSC", "ethereum": "Ethereum", "base": "Base"}
GT_NET = {"solana": "solana", "bsc": "bsc", "ethereum": "eth", "base": "base",
          "arbitrum": "arbitrum", "polygon": "polygon_pos", "avalanche": "avax", "optimism": "optimism"}
BINANCE = ["https://data-api.binance.vision", "https://api.binance.com"]
STABLES = {"USDT", "USDC", "FDUSD", "TUSD", "DAI", "BUSD", "USDP", "USDE", "EUR", "TRY", "BRL", "WBTC", "WBETH"}
NARRATIVES = {
    "AI": ["ai", "gpt", "agent", "agents", "neural", "llm", "bot"],
    "Meme-Dog": ["dog", "doge", "inu", "shib", "wif", "bonk", "puppy"],
    "Meme-Cat": ["cat", "kitty", "meow", "popcat", "mog"],
    "Meme-Frog": ["pepe", "frog", "kek"],
    "Politics": ["trump", "maga", "biden", "elon", "musk"],
    "Gaming": ["game", "gaming", "play", "arcade"],
    "RWA": ["rwa", "gold", "treasury"],
}

S = requests.Session()
S.headers.update({"User-Agent": "Mozilla/5.0 (momentum-scanner-bot)"})


def log(msg):
    msg = str(msg)
    if TG_TOKEN:
        msg = msg.replace(TG_TOKEN, "***")
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


# ---------------- State (persisted in state.json) ----------------
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


# ---------------- Telegram ----------------
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


HELP = ("🤖 Commands\n"
        "/scan - run a full scan now\n"
        "/coin <contract address> - analyze any token\n"
        "/stats - performance of past picks\n"
        "/help - this message\n\n"
        "Note: commands are answered at the next bot cycle (up to ~15 min on GitHub Actions).")


def handle_commands(st):
    cmds = {"scan": False, "coins": [], "stats": False, "help": False}
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
        elif cmd in ("/help", "/start"):
            cmds["help"] = True
    return cmds


# ---------------- Indicators (pure Python) ----------------
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


# ---------------- Context: BTC regime, trending, TVL ----------------
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


def chain_context():
    out = []
    for ch in CHAINS:
        name = LLAMA_NAMES.get(ch)
        if not name:
            continue
        d = get_json(f"https://api.llama.fi/v2/historicalChainTvl/{name}")
        if isinstance(d, list) and len(d) > 8:
            now, wk = d[-1]["tvl"], d[-8]["tvl"]
            if wk:
                out.append(f"{name} TVL ${now / 1e9:.2f}B ({(now / wk - 1) * 100:+.1f}% 7d)")
        time.sleep(0.4)
    return out


def build_ctx():
    syms, cats = coingecko_trending()
    return {"regime": btc_regime(), "trend_syms": syms, "hot_cats": cats, "tvl": chain_context()}


def narrative_tags(name, symbol):
    toks = re.findall(r"[a-z0-9]+", f"{name} {symbol}".lower())
    out = []
    for theme, words in NARRATIVES.items():
        if any(t == w or (len(w) >= 4 and w in t) for t in toks for w in words):
            out.append(theme)
    return out


# ---------------- Safety: honeypot / holders / dev / LP ----------------
def safety_check(p):
    """returns (penalty, flags, block, infos)"""
    ch, addr = p["chainId"], p["baseToken"]["address"]
    flags, infos, pen, block, ok = [], [], 0, False, False
    if ch == "solana":
        d = get_json(f"https://api.rugcheck.xyz/v1/tokens/{addr}/report")
        if isinstance(d, dict):
            ok = True
            risks = d.get("risks") or []
            danger = [r.get("name") for r in risks if str(r.get("level", "")).lower() == "danger"]
            warn = [r.get("name") for r in risks if str(r.get("level", "")).lower() == "warn"]
            for n in danger[:3]:
                flags.append(f"Danger: {n}")
            pen += 12 * len(danger)
            if len(danger) >= 2:
                block = True
            for n in warn[:2]:
                flags.append(f"Warning: {n}")
                pen += 3
            if d.get("rugged"):
                block = True
                flags.append("RUGGED")
            holders = d.get("topHolders") or []
            if holders:
                top10 = sum(_f(h.get("pct")) for h in holders[:10])
                infos.append(f"Top 10 holders own {top10:.0f}% (may include liquidity pool)")
                if top10 > 60:
                    flags.append(f"High holder concentration ({top10:.0f}%)")
                    pen += 12
                elif top10 > 40:
                    flags.append(f"Concentrated holders ({top10:.0f}%)")
                    pen += 6
                insiders = sum(1 for h in holders[:10] if h.get("insider"))
                if insiders >= 3:
                    flags.append(f"{insiders} insider-linked wallets in top 10")
                    pen += 8
                creator = d.get("creator")
                if creator:
                    cp = sum(_f(h.get("pct")) for h in holders if creator in (h.get("address"), h.get("owner")))
                    if cp > 10:
                        flags.append(f"Creator wallet holds {cp:.0f}%")
                        pen += 8
                    elif cp == 0:
                        infos.append("Creator wallet not in top holders (sold or distributed)")
    elif ch == "bsc":
        g = get_json("https://api.gopluslabs.io/api/v1/token_security/56", params={"contract_addresses": addr})
        try:
            r = g["result"][addr.lower()]
        except Exception:  # noqa
            r = None
        if r:
            ok = True
            if str(r.get("is_honeypot")) == "1":
                block = True
                flags.append("HONEYPOT")
            sell_tax, buy_tax = _f(r.get("sell_tax")) * 100, _f(r.get("buy_tax")) * 100
            if sell_tax > 30 or buy_tax > 30:
                block = True
            if sell_tax > 10:
                flags.append(f"Sell tax {sell_tax:.0f}%")
                pen += 15
            if buy_tax > 10:
                flags.append(f"Buy tax {buy_tax:.0f}%")
                pen += 5
            for k, msg, pn in (("cannot_sell_all", "Cannot sell all tokens", 15),
                               ("is_mintable", "Owner can mint new tokens", 8),
                               ("hidden_owner", "Hidden owner", 10),
                               ("can_take_back_ownership", "Owner can reclaim ownership", 10),
                               ("owner_change_balance", "Owner can change balances", 20),
                               ("transfer_pausable", "Transfers can be paused", 8),
                               ("is_blacklisted", "Has blacklist function", 5)):
                if str(r.get(k)) == "1":
                    flags.append(msg)
                    pen += pn
            if str(r.get("is_open_source")) == "0":
                flags.append("Contract not verified")
                pen += 10
            wallets = [h for h in (r.get("holders") or []) if str(h.get("is_contract")) != "1"]
            if wallets:
                top = sum(_f(h.get("percent")) for h in wallets[:10]) * 100
                infos.append(f"Top wallets hold {top:.0f}% (excl. contracts)")
                if top > 40:
                    flags.append(f"High holder concentration ({top:.0f}%)")
                    pen += 10
                elif top > 25:
                    flags.append(f"Concentrated holders ({top:.0f}%)")
                    pen += 5
            cp, op = _f(r.get("creator_percent")) * 100, _f(r.get("owner_percent")) * 100
            dev = max(cp, op)
            if dev > 10:
                flags.append(f"Dev/owner wallet holds {dev:.0f}%")
                pen += 8
            elif r.get("creator_percent") not in (None, "") and dev == 0:
                infos.append("Dev/owner wallet holds 0% (sold or renounced)")
            lp = r.get("lp_holders") or []
            if lp:
                locked = sum(_f(h.get("percent")) for h in lp if str(h.get("is_locked")) == "1") * 100
                if locked < 50:
                    flags.append(f"LP only {locked:.0f}% locked")
                    pen += 8
                else:
                    infos.append(f"LP {locked:.0f}% locked")
            hc = _f(r.get("holder_count"))
            if 0 < hc < 100:
                flags.append(f"Only {hc:.0f} holders")
                pen += 5
        else:
            d = get_json("https://api.honeypot.is/v2/IsHoneypot", params={"address": addr, "chainID": 56})
            if isinstance(d, dict):
                ok = True
                if (d.get("honeypotResult") or {}).get("isHoneypot"):
                    block = True
                    flags.append("HONEYPOT")
                tax = (d.get("simulationResult") or {}).get("sellTax")
                if tax is not None and tax > 10:
                    flags.append(f"Sell tax {tax:.0f}%")
                    pen += 15
                    block = block or tax > 30
    if not ok:
        flags.append("Safety check unavailable (check manually)")
    liq = (p.get("liquidity") or {}).get("usd") or 0
    mc = p.get("marketCap") or p.get("fdv") or 0
    if liq and mc and mc / liq > 50:
        flags.append("Thin liquidity vs market cap")
        pen += 8
    return pen, flags, block, infos


# ---------------- DEX data ----------------
def collect_candidates():
    cands = set()
    for url in ("https://api.dexscreener.com/token-boosts/top/v1",
                "https://api.dexscreener.com/token-boosts/latest/v1",
                "https://api.dexscreener.com/token-profiles/latest/v1"):
        data = get_json(url)
        if isinstance(data, list):
            for t in data:
                ch, addr = t.get("chainId"), t.get("tokenAddress")
                if ch in CHAINS and addr:
                    cands.add((ch, addr))
        time.sleep(0.5)
    for ch in CHAINS:
        data = get_json(f"https://api.geckoterminal.com/api/v2/networks/{GT_NET.get(ch, ch)}/trending_pools")
        try:
            for pool in data["data"]:
                tid = pool["relationships"]["base_token"]["data"]["id"]
                cands.add((ch, tid.split("_", 1)[1]))
        except Exception:  # noqa
            pass
        time.sleep(GT_DELAY)
    log(f"candidates: {len(cands)}")
    return cands


def fetch_pairs(cands):
    by_chain = {}
    for ch, addr in cands:
        by_chain.setdefault(ch, []).append(addr)
    best = {}
    for ch, addrs in by_chain.items():
        for i in range(0, len(addrs), 30):
            data = get_json(f"https://api.dexscreener.com/tokens/v1/{ch}/{','.join(addrs[i:i + 30])}")
            if not isinstance(data, list):
                continue
            for p in data:
                try:
                    key = (ch, p["baseToken"]["address"])
                    liq = (p.get("liquidity") or {}).get("usd") or 0
                    if key not in best or liq > best[key][1]:
                        best[key] = (p, liq)
                except KeyError:
                    continue
            time.sleep(0.4)
    return [v[0] for v in best.values()]


def basic_filter(p):
    liq = (p.get("liquidity") or {}).get("usd") or 0
    vol = (p.get("volume") or {}).get("h24") or 0
    created = p.get("pairCreatedAt")
    age_h = (time.time() * 1000 - created) / 3.6e6 if created else 9999
    return liq >= MIN_LIQ and vol >= MIN_VOL and age_h >= MIN_AGE_H


def gt_candles(p, tf, agg, limit):
    ch = p["chainId"]
    d = get_json(f"https://api.geckoterminal.com/api/v2/networks/{GT_NET.get(ch, ch)}/pools/{p.get('pairAddress')}/ohlcv/{tf}",
                 params={"aggregate": agg, "limit": limit})
    try:
        rows = sorted(([_f(x) for x in r] for r in d["data"]["attributes"]["ohlcv_list"]), key=lambda r: r[0])
    except Exception:  # noqa
        return None
    return rows if len(rows) >= 30 else None


def market_score(p):
    s, notes, tags = 0.0, [], []
    v, tx, pc = p.get("volume") or {}, p.get("txns") or {}, p.get("priceChange") or {}
    liq = (p.get("liquidity") or {}).get("usd") or 0
    h1, h24 = v.get("h1") or 0, v.get("h24") or 0
    ratio = h1 / (h24 / 24) if h24 else 0
    s += min(ratio, 4) / 4 * W["vol_spike"]
    if ratio >= 2:
        tags.append("vol_spike")
        notes.append(f"Volume spike {ratio:.1f}x (1h vs 24h avg)")
    t1 = tx.get("h1") or {}
    b, se = t1.get("buys", 0), t1.get("sells", 0)
    if b + se >= 20:
        bp = b / (b + se)
        s += min(max(bp - 0.5, 0) / 0.2, 1) * W["buy_pressure"]
        if bp >= 0.6:
            tags.append("buy_pressure")
            notes.append(f"Buy pressure {bp * 100:.0f}% ({b} buys / {se} sells, 1h)")
    turnover = h24 / liq if liq else 0
    if 0.5 <= turnover <= 8:
        s += W["turnover"]
    elif turnover > 15:
        s -= 5
        notes.append("Very high turnover vs liquidity (overheated?)")
    c1, c6, c24 = pc.get("h1") or 0, pc.get("h6") or 0, pc.get("h24") or 0
    if 0 < c1 <= 30:
        s += W["price_ok"] / 2
    if 0 < c6 <= 80:
        s += W["price_ok"] / 2
    if c24 > 300:
        s -= 15
        tags.append("already_pumped")
        notes.append(f"Already pumped +{c24:.0f}% in 24h (late entry risk)")
    elif c24 < -30:
        s -= 5
    return max(s, 0), notes, tags


def evaluate_dex(p, ctx, keep_blocked=False):
    ms, mn, mt = market_score(p)
    h1 = gt_candles(p, "hour", 1, 200)
    time.sleep(GT_DELAY)
    m15 = gt_candles(p, "minute", 15, 100)
    time.sleep(GT_DELAY)
    h4 = to_4h(h1) if h1 else None
    ts, tn, tw, tt, info = tech_analysis(h1, h4, m15)
    pen, flags, block, infos = safety_check(p)
    sym, name = p["baseToken"].get("symbol", "?"), p["baseToken"].get("name", "")
    if block and not keep_blocked:
        log(f"blocked {sym}: {flags}")
        return None
    notes, tags, bonus = mn + tn, mt + tt + [f"chain:{p['chainId']}", "dex"], 0
    if sym.upper() in ctx["trend_syms"]:
        bonus += W["trending"]
        notes.append("Trending on CoinGecko")
        tags.append("cg_trending")
    themes = narrative_tags(name, sym)
    tags += [f"theme:{t}" for t in themes]
    score = max(0.0, min(100.0, (ms + ts + bonus - pen) * ctx["regime"]["factor"]))
    v = p.get("volume") or {}
    ratio = (v.get("h1") or 0) / ((v.get("h24") or 0) / 24) if v.get("h24") else 0
    if m15 and len(m15) >= 30:
        v15 = col(m15, 5)
        base15 = sum(v15[-27:-3]) / 24
        if base15:
            ratio = max(ratio, (sum(v15[-3:-1]) / 2) / base15)
    stage = entry_stage((p.get("priceChange") or {}).get("h24") or 0, ratio, info, late_c24=60, early_c24=15)
    tags.append(f"stage:{stage}")
    if stage == "LATE":
        score = min(score, ALERT_MIN_SCORE - 1)  # the pump already happened: never alert
    if block:
        flags.insert(0, "🚫 Scam risk detected - avoid")
        score = min(score, 15)
    pc, mc = p.get("priceChange") or {}, p.get("marketCap") or p.get("fdv")
    price = _f(p.get("priceUsd"))
    lines = [f"Price ${price:.8g} | 1h {pc.get('h1', 0):+.1f}% | 24h {pc.get('h24', 0):+.1f}%",
             f"Liq {usd((p.get('liquidity') or {}).get('usd'))} | Vol24h {usd((p.get('volume') or {}).get('h24'))} | MC {usd(mc)}"]
    for extra in (tech_line(info), sr_line(info), ("Theme: " + ", ".join(themes)) if themes else ""):
        if extra:
            lines.append(extra)
    return {"kind": "dex", "key": f"{p['chainId']}:{p['baseToken']['address'].lower()}", "symbol": sym,
            "chain": p["chainId"], "addr": p["baseToken"]["address"], "title": f"{sym} ({p['chainId']}) [{stage}]", "stage": stage,
            "score": score, "price": price, "lines": lines, "notes": notes, "flags": flags + tw,
            "infos": infos, "tags": tags, "url": p.get("url", "")}


# ---------------- CEX (large caps) ----------------
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


def kl_rows(kl):
    return [[_f(k[0]) / 1000, _f(k[1]), _f(k[2]), _f(k[3]), _f(k[4]), _f(k[7])] for k in kl]


def evaluate_cex(t, ctx, st):
    sym = t["symbol"]
    base = sym[:-4]
    k1, k15, k4 = (binance_klines(sym, "1h", 200), binance_klines(sym, "15m", 100), binance_klines(sym, "4h", 100))
    if not k1 or len(k1) < 50:
        return None
    h1, m15, h4 = kl_rows(k1), kl_rows(k15) if k15 else None, kl_rows(k4) if k4 else None
    ts, tn, tw, tt, info = tech_analysis(h1, h4, m15)
    notes, warns, tags = list(tn), list(tw), tt + ["cex"]
    ms = 0.0
    vols = col(h1, 5)
    ratio = vols[-2] / (sum(vols[-26:-2]) / 24) if len(vols) >= 27 and sum(vols[-26:-2]) else 0
    ms += min(ratio, 4) / 4 * W["vol_spike"]
    if ratio >= 2:
        tags.append("vol_spike")
        notes.append(f"Volume spike {ratio:.1f}x (last closed 1h vs 24h avg)")
    c24 = _f(t.get("priceChangePercent"))
    if 2 <= c24 <= 25:
        ms += 10
    elif c24 > 40:
        ms -= 10
        warns.append(f"Already +{c24:.0f}% in 24h")
    fr, oi = okx_funding_oi(base)
    fund_txt = ""
    if fr is not None:
        fund_txt = f"Funding {fr * 100:+.3f}%"
        if fr > 0.0005:
            ms -= 8
            warns.append(f"Funding {fr * 100:.3f}% (crowded longs, squeeze-down risk)")
        elif fr < 0 and c24 > 0:
            ms += 5
            tags.append("neg_funding_up")
            notes.append("Negative funding while price rises (short-squeeze potential)")
    oi_txt = ""
    if oi:
        oi_txt = f"OI {usd(oi)}"
        prev = st["oi"].get(base)
        if prev and prev.get("v"):
            chg = (oi / prev["v"] - 1) * 100
            oi_txt += f" ({chg:+.1f}% since last scan)"
            if chg > 5 and c24 > 0:
                ms += 5
                tags.append("oi_up")
                notes.append(f"Open interest +{chg:.1f}% with rising price")
        st["oi"][base] = {"v": oi, "ts": time.time()}
    ms *= 1.3  # CEX has fewer market sub-scores; scale to a comparable range
    bonus = 0
    if base.upper() in ctx["trend_syms"]:
        bonus = W["trending"]
        notes.append("Trending on CoinGecko")
        tags.append("cg_trending")
    score = max(0.0, min(100.0, (ms + ts + bonus) * ctx["regime"]["factor"]))
    stage = entry_stage(c24, ratio, info)
    tags.append(f"stage:{stage}")
    if stage == "LATE":
        score = min(score, ALERT_MIN_SCORE - 1)  # the pump already happened: never alert
    price = _f(t.get("lastPrice"))
    lines = [f"Price ${price:.6g} | 24h {c24:+.1f}% | QVol {usd(_f(t.get('quoteVolume')))}"]
    extra = " | ".join(x for x in (fund_txt, oi_txt) if x)
    for e in (extra, tech_line(info), sr_line(info)):
        if e:
            lines.append(e)
    return {"kind": "cex", "key": f"cex:{base}", "symbol": base, "chain": "cex", "addr": sym,
            "title": f"{base} (Binance) [{stage}]", "stage": stage, "score": score, "price": price, "lines": lines, "notes": notes,
            "flags": warns, "infos": [], "tags": tags, "url": f"https://www.binance.com/en/trade/{base}_USDT"}


def scan_cex(ctx, st):
    tick = None
    for base in BINANCE:
        tick = get_json(f"{base}/api/v3/ticker/24hr", retries=1)
        if isinstance(tick, list):
            break
    if not isinstance(tick, list):
        log("CEX ticker unavailable")
        return []
    cands = []
    for t in tick:
        sym = t.get("symbol", "")
        if not sym.endswith("USDT") or sym[:-4] in STABLES:
            continue
        qv, ch = _f(t.get("quoteVolume")), _f(t.get("priceChangePercent"))
        if qv >= CEX_MIN_QV and 1 <= ch <= 60:
            cands.append((qv, t))
    cands.sort(key=lambda x: x[0], reverse=True)
    items = []
    for _, t in cands[:CEX_N]:
        try:
            it = evaluate_cex(t, ctx, st)
        except Exception as e:  # noqa
            log(f"cex eval failed {t.get('symbol')}: {e}")
            it = None
        if it:
            items.append(it)
        time.sleep(0.3)
    items.sort(key=lambda x: x["score"], reverse=True)
    return items


# ---------------- Scan ----------------
def scan(ctx, st):
    pairs = [p for p in fetch_pairs(collect_candidates()) if basic_filter(p)]
    log(f"passed basic filter: {len(pairs)}")
    for p in pairs:
        p["_ms"] = market_score(p)[0]
    pairs.sort(key=lambda x: x["_ms"], reverse=True)
    dex = []
    for p in pairs[:SHORTLIST]:
        try:
            it = evaluate_dex(p, ctx)
        except Exception as e:  # noqa
            log(f"dex eval failed: {e}")
            it = None
        if it:
            dex.append(it)
    dex.sort(key=lambda x: x["score"], reverse=True)
    cex = scan_cex(ctx, st) if ENABLE_CEX else []
    return dex[:TOP_N], cex[:3]


def analyze_coin(q, ctx):
    q = q.strip()
    if len(q) < 30 and not q.lower().startswith("0x"):
        return "Please send the full contract address: /coin <address>"
    d = get_json("https://api.dexscreener.com/latest/dex/search", params={"q": q})
    pairs = [p for p in ((d or {}).get("pairs") or []) if p.get("chainId") in GT_NET]
    exact = [p for p in pairs if (p.get("baseToken") or {}).get("address", "").lower() == q.lower()]
    pool = exact or pairs
    if not pool:
        return "Token not found on DexScreener."
    p = max(pool, key=lambda x: (x.get("liquidity") or {}).get("usd") or 0)
    it = evaluate_dex(p, ctx, keep_blocked=True)
    return fmt_item(it) + f"\n\n{ctx['regime']['note']}\n\n{DISCLAIMER}"


# ---------------- Reports / alerts ----------------
DISCLAIMER = ("⚠️ Not financial advice. The score reflects momentum and a risk filter only, "
              "not a guarantee of a pump. Always DYOR.")


def fmt_item(it, tag=""):
    lines = [f"{it['title']} - Score {it['score']:.0f}/100 {tag}".strip()]
    lines += it["lines"]
    lines += [f"✅ {n}" for n in it["notes"][:5]]
    lines += [f"ℹ️ {n}" for n in it["infos"][:3]]
    lines += [f"⚠️ {n}" for n in it["flags"][:6]]
    if it.get("url"):
        lines.append(it["url"])
    return "\n".join(lines)


def header(ctx):
    now = datetime.now(timezone.utc).strftime("%d %b %Y %H:%M UTC")
    out = [f"📊 Momentum Scan - {now}", ctx["regime"]["note"]]
    if ctx["tvl"]:
        out.append("Market: " + " | ".join(ctx["tvl"]))
    if ctx["hot_cats"]:
        out.append("CoinGecko hot categories: " + ", ".join(ctx["hot_cats"]))
    return "\n".join(out)


def full_report(ctx, dex, cex):
    blocks = [header(ctx)]
    if not dex and not cex:
        blocks.append("No coins passed the filters right now.")
    if dex:
        blocks.append("🔹 DEX coins (Solana/BNB)")
        blocks += [f"{i}) " + fmt_item(it) for i, it in enumerate(dex, 1)]
    if cex:
        blocks.append("🔸 Large caps (Binance, funding/OI)")
        blocks += [f"{i}) " + fmt_item(it) for i, it in enumerate(cex, 1)]
    blocks.append(DISCLAIMER)
    return blocks


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


# ---------------- Performance tracking ----------------
CHECKS = (("1h", 3600), ("6h", 21600), ("24h", 86400))


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


# ---------------- Twitter (optional) ----------------
def maybe_tweet(st, items):
    if not POST_TWITTER or time.time() - st.get("last_tweet", 0) < TWEET_EVERY_H * 3600:
        return
    top = [i for i in items if i["kind"] == "dex" and i["score"] >= TWEET_MIN_SCORE][:3]
    if not top:
        return
    names = ", ".join(f"{i['symbol']} ({i['chain']})" for i in top)
    text = (f"🔎 Momentum watchlist: {names}\n"
            "Volume + buy pressure rising. Not financial advice, DYOR. High scam risk in new tokens.")
    try:
        import tweepy
        client = tweepy.Client(consumer_key=os.getenv("X_API_KEY"), consumer_secret=os.getenv("X_API_SECRET"),
                               access_token=os.getenv("X_ACCESS_TOKEN"),
                               access_token_secret=os.getenv("X_ACCESS_TOKEN_SECRET"))
        client.create_tweet(text=text[:280])
        st["last_tweet"] = time.time()
        log("tweet posted")
    except Exception as e:  # noqa
        log(f"tweet failed: {e}")


def only_new(st, items):
    """coins not reported within the cooldown, or whose score jumped by ALERT_JUMP since the last report"""
    now, out = time.time(), []
    for it in items:
        a = st["alerted"].get(it["key"])
        if a is None or now - a["ts"] > ALERT_COOLDOWN_H * 3600 or it["score"] >= a["score"] + ALERT_JUMP:
            out.append(it)
    return out


def digest_pick(items):
    """coins that passed: good score and the pump has not already happened"""
    ok = [i for i in items if i["score"] >= DIGEST_MIN_SCORE and i.get("stage", "RUNNING") != "LATE"]
    return sorted(ok, key=lambda i: i["score"], reverse=True)[:TOP_N]


def digest_blocks(ctx, passed):
    blocks = [header(ctx)]
    if passed:
        title = f"🆕 {len(passed)} new or stronger coin(s) found" if DIGEST_ONLY_NEW else f"⏰ Hourly signals: {len(passed)} coin(s) passed"
        blocks.append(f"{title} (score >= {DIGEST_MIN_SCORE:.0f}, not LATE)")
        blocks += [f"{i}) " + fmt_item(it) for i, it in enumerate(passed, 1)]
    else:
        blocks.append("⏰ Hourly scan: no " + ("new " if DIGEST_ONLY_NEW else "") +
                      f"coin passed this hour (score >= {DIGEST_MIN_SCORE:.0f}, not LATE). Bot is running normally.")
    blocks.append(DISCLAIMER)
    return blocks


# ---------------- Main cycle ----------------
def tick(force=False):
    st = load_state()
    ctx_cache = []

    def get_ctx():
        if not ctx_cache:
            ctx_cache.append(build_ctx())
        return ctx_cache[0]

    try:
        cmds = handle_commands(st)
        if cmds["help"]:
            send_telegram(HELP)
        update_perf(st)
        if cmds["stats"]:
            send_telegram(perf_report(st))
        for q in cmds["coins"][:3]:
            try:
                send_telegram(analyze_coin(q, get_ctx()))
            except Exception as e:  # noqa
                log(f"/coin failed: {e}")
                send_telegram("Could not analyze that token right now. Try again later.")
        now = time.time()
        full = force or cmds["scan"]
        if full or now - st["last_scan"] >= SCAN_EVERY_MIN * 60 - 120:
            ctx = get_ctx()
            dex, cex = scan(ctx, st)
            items = dex + cex
            st["last_scan"], st["n_scans"] = now, st["n_scans"] + 1
            digest_due = now - st.get("last_digest", 0) >= DIGEST_EVERY_MIN * 60 - 120
            if full:
                st["last_digest"] = now
                send_blocks(full_report(ctx, dex, cex))
                record_picks(st, items)
                mark_alerted(st, [i for i in items if i["score"] >= ALERT_MIN_SCORE])
                maybe_tweet(st, items)
            elif digest_due:
                st["last_digest"] = now
                passed = digest_pick(items)
                if DIGEST_ONLY_NEW:
                    passed = only_new(st, passed)
                if passed or DIGEST_SEND_EMPTY:
                    send_blocks(digest_blocks(ctx, passed))
                record_picks(st, passed)
                mark_alerted(st, passed)
                maybe_tweet(st, passed)
            else:
                alerts = pick_alerts(st, items)
                if alerts:
                    blocks = [header(ctx), "🚨 Momentum alert"]
                    blocks += [fmt_item(it, tag) for it, tag in alerts]
                    blocks.append(DISCLAIMER)
                    send_blocks(blocks)
                    record_picks(st, [it for it, _ in alerts])
                    mark_alerted(st, [it for it, _ in alerts])
                    st["n_alerts"] += len(alerts)
                    maybe_tweet(st, [it for it, _ in alerts])
                else:
                    log("no new alerts")
        if st["last_summary"] == 0:
            st["last_summary"] = now
        elif now - st["last_summary"] >= 86400:
            send_blocks([f"🟢 Daily summary: {st['n_scans']} scans, {st['n_alerts']} alerts in the last 24h.",
                         perf_report(st)])
            st["n_scans"] = st["n_alerts"] = 0
            st["last_summary"] = now
    finally:
        save_state(st)


def watch(minutes, force_first=False):
    """Keep running tick() every TICK_SECONDS for `minutes` (used by GitHub Actions for near-real-time)."""
    end = time.time() + minutes * 60
    first = True
    while True:
        try:
            tick(force=force_first and first)
        except Exception as e:  # noqa
            log(f"tick error: {e}")
        first = False
        if time.time() + TICK_SECONDS >= end:
            break
        time.sleep(TICK_SECONDS)


if __name__ == "__main__":
    if "--test-telegram" in sys.argv:
        print("OK" if send_telegram("✅ Bot connected! Telegram setup is working.") else "FAILED - check your token/chat id")
    elif "--once" in sys.argv:
        tick(force=True)
    elif "--watch" in sys.argv:
        mins = float(sys.argv[sys.argv.index("--watch") + 1])
        watch(mins, force_first="--force" in sys.argv)
    elif "--tick" in sys.argv:
        tick(force="--force" in sys.argv)
    else:
        while True:
            try:
                tick()
            except Exception as e:  # noqa
                log(f"error: {e}")
            time.sleep(TICK_SECONDS)
