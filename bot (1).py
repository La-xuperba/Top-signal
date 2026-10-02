"""
Crypto Momentum Scanner (Solana + BNB)
- Free public endpoints only (no paid API): DexScreener, GeckoTerminal, DefiLlama,
  RugCheck (Solana), Honeypot.is (BNB)
- Scores coins on market activity + technical indicators + risk flags
- Sends report to Telegram, optionally posts a short watchlist on X (Twitter)

Eta "pump hobe" er guarantee na. Eta shudhu momentum + risk filter score.

Run:
  python bot.py --test-telegram   # connection test
  python bot.py --once            # ekbar scan kore report pathabe
  python bot.py                   # loop-e cholbe (SCAN_MINUTES por por)
"""
import json
import os
import sys
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

load_dotenv()

# ---------- Settings (.env theke) ----------
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")
CHAINS = [c.strip() for c in os.getenv("CHAINS", "solana,bsc").split(",") if c.strip()]
SCAN_MINUTES = int(os.getenv("SCAN_MINUTES", "30"))
TOP_N = int(os.getenv("TOP_N", "5"))
MIN_LIQ = float(os.getenv("MIN_LIQUIDITY_USD", "30000"))
MIN_VOL = float(os.getenv("MIN_VOLUME_24H_USD", "50000"))
MIN_AGE_H = float(os.getenv("MIN_AGE_HOURS", "6"))
POST_TWITTER = os.getenv("POST_TO_TWITTER", "false").lower() == "true"
TWEET_EVERY_H = float(os.getenv("TWEET_EVERY_HOURS", "6"))
TWEET_MIN_SCORE = float(os.getenv("TWEET_MIN_SCORE", "65"))
STATE_FILE = "state.json"

LLAMA_NAMES = {"solana": "Solana", "bsc": "BSC", "ethereum": "Ethereum", "base": "Base"}

S = requests.Session()
S.headers.update({"User-Agent": "Mozilla/5.0 (momentum-scanner-bot)"})


def log(msg):
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
                log(f"request failed: {url} -> {e}")
                return None
            time.sleep(2)
    return None


# ---------- 1) Candidate collection ----------
def collect_candidates():
    cands = set()
    for url in (
        "https://api.dexscreener.com/token-boosts/top/v1",
        "https://api.dexscreener.com/token-boosts/latest/v1",
        "https://api.dexscreener.com/token-profiles/latest/v1",
    ):
        data = get_json(url)
        if isinstance(data, list):
            for t in data:
                ch, addr = t.get("chainId"), t.get("tokenAddress")
                if ch in CHAINS and addr:
                    cands.add((ch, addr))
        time.sleep(0.5)

    for ch in CHAINS:
        data = get_json(f"https://api.geckoterminal.com/api/v2/networks/{ch}/trending_pools")
        try:
            for pool in data["data"]:
                tid = pool["relationships"]["base_token"]["data"]["id"]  # e.g. solana_ADDRESS
                cands.add((ch, tid.split("_", 1)[1]))
        except Exception:  # noqa
            pass
        time.sleep(2.5)
    log(f"candidates: {len(cands)}")
    return cands


def fetch_pairs(cands):
    by_chain = {}
    for ch, addr in cands:
        by_chain.setdefault(ch, []).append(addr)
    best = {}
    for ch, addrs in by_chain.items():
        for i in range(0, len(addrs), 30):
            chunk = addrs[i:i + 30]
            data = get_json(f"https://api.dexscreener.com/tokens/v1/{ch}/{','.join(chunk)}")
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


# ---------- 2) Market / on-chain activity score (max 60) ----------
def market_score(p):
    s, notes = 0.0, []
    v, tx, pc = p.get("volume") or {}, p.get("txns") or {}, p.get("priceChange") or {}
    liq = (p.get("liquidity") or {}).get("usd") or 0

    h1, h24 = v.get("h1") or 0, v.get("h24") or 0
    ratio = h1 / (h24 / 24) if h24 else 0
    s += min(ratio, 4) / 4 * 25
    if ratio >= 2:
        notes.append(f"Volume spike {ratio:.1f}x (1h vs 24h avg)")

    t1 = tx.get("h1") or {}
    b, se = t1.get("buys", 0), t1.get("sells", 0)
    if b + se >= 20:
        bp = b / (b + se)
        s += min(max(bp - 0.5, 0) / 0.2, 1) * 15
        if bp >= 0.6:
            notes.append(f"Buy pressure {bp * 100:.0f}% ({b} buys / {se} sells, 1h)")

    turnover = h24 / liq if liq else 0
    if 0.5 <= turnover <= 8:
        s += 10
    elif turnover > 15:
        s -= 5
        notes.append("Very high turnover vs liquidity (overheated?)")

    c1, c6, c24 = pc.get("h1") or 0, pc.get("h6") or 0, pc.get("h24") or 0
    if 0 < c1 <= 30:
        s += 5
    if 0 < c6 <= 80:
        s += 5
    if c24 > 300:
        s -= 15
        notes.append(f"Already pumped +{c24:.0f}% in 24h (late entry risk)")
    elif c24 < -30:
        s -= 5
    return max(s, 0), notes


# ---------- 3) Technical analysis (max 40) ----------
def fetch_candles(p):
    pool = p.get("pairAddress")
    d = get_json(
        f"https://api.geckoterminal.com/api/v2/networks/{p['chainId']}/pools/{pool}/ohlcv/hour",
        params={"aggregate": 1, "limit": 120},
    )
    try:
        rows = d["data"]["attributes"]["ohlcv_list"]
    except Exception:  # noqa
        return None
    if not rows or len(rows) < 50:
        return None
    rows = sorted(rows, key=lambda r: r[0])
    return {"h": [float(r[2]) for r in rows], "c": [float(r[4]) for r in rows]}


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
    rs = up / (dn if dn else 1e-12)
    return 100 - 100 / (1 + rs)


def macd_hist(values):
    m = [a - b for a, b in zip(ema(values, 12), ema(values, 26))]
    return [x - y for x, y in zip(m, ema(m, 9))]


def tech_score(cd):
    c = cd["c"]
    s, notes, info = 0.0, [], {}
    r = rsi(c)
    info["rsi"] = r
    if 50 <= r <= 68:
        s += 12
        notes.append(f"RSI {r:.0f} (healthy momentum)")
    elif 40 <= r < 50:
        s += 5
    elif r > 75:
        s -= 8
        notes.append(f"RSI {r:.0f} (overbought)")

    h = macd_hist(c)
    if h[-1] > 0 and h[-1] > h[-2]:
        s += 10
        info["macd"] = "up"
        notes.append("MACD bullish and rising")
    elif h[-1] > 0:
        s += 5
        info["macd"] = "+"
    else:
        info["macd"] = "down"

    e20, e50 = ema(c, 20)[-1], ema(c, 50)[-1]
    if c[-1] > e20 > e50:
        s += 10
        notes.append("Price > EMA20 > EMA50 (uptrend)")

    if c[-1] > max(cd["h"][-25:-1]):
        s += 8
        info["breakout"] = True
        notes.append("Broke above 24h high")
    return s, notes, info


# ---------- 4) Risk check (free endpoints) ----------
def risk_check(p):
    ch, addr = p["chainId"], p["baseToken"]["address"]
    flags, penalty, block, d = [], 0, False, None
    if ch == "solana":
        d = get_json(f"https://api.rugcheck.xyz/v1/tokens/{addr}/report/summary")
        if d:
            danger = [r.get("name") for r in (d.get("risks") or [])
                      if str(r.get("level", "")).lower() == "danger"]
            for n in danger[:3]:
                flags.append(f"Danger: {n}")
            penalty += 12 * len(danger)
            if len(danger) >= 2:
                block = True
    elif ch == "bsc":
        d = get_json("https://api.honeypot.is/v2/IsHoneypot", params={"address": addr, "chainID": 56})
        if d:
            if (d.get("honeypotResult") or {}).get("isHoneypot"):
                block = True
                flags.append("HONEYPOT")
            sell_tax = (d.get("simulationResult") or {}).get("sellTax")
            if sell_tax is not None:
                if sell_tax > 30:
                    block = True
                if sell_tax > 10:
                    flags.append(f"Sell tax {sell_tax:.0f}%")
                    penalty += 15
    if not d:
        flags.append("Risk check unavailable (manual check korun)")
    liq = (p.get("liquidity") or {}).get("usd") or 0
    mc = p.get("marketCap") or p.get("fdv") or 0
    if liq and mc and mc / liq > 50:
        flags.append("Thin liquidity vs market cap")
        penalty += 8
    return penalty, flags, block


# ---------- 5) Chain context (DefiLlama) ----------
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


# ---------- Scan ----------
def scan():
    pairs = [p for p in fetch_pairs(collect_candidates()) if basic_filter(p)]
    log(f"passed basic filter: {len(pairs)}")
    for p in pairs:
        p["_ms"], p["_notes"] = market_score(p)
    pairs.sort(key=lambda x: x["_ms"], reverse=True)

    results = []
    for p in pairs[:15]:
        df = fetch_candles(p)
        time.sleep(2.5)  # GeckoTerminal free rate limit
        if df is not None:
            ts, tn, info = tech_score(df)
        else:
            ts, tn, info = 0, ["Candle data pawa jayni"], {}
        penalty, flags, block = risk_check(p)
        if block:
            log(f"blocked {p['baseToken']['symbol']}: {flags}")
            continue
        total = max(0, min(100, p["_ms"] + ts - penalty))
        results.append({"p": p, "score": total, "notes": p["_notes"] + tn, "flags": flags, "info": info})
    results.sort(key=lambda r: r["score"], reverse=True)
    return results[:TOP_N]


# ---------- Output ----------
def usd(x):
    x = x or 0
    for div, suf in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(x) >= div:
            return f"${x / div:.2f}{suf}"
    return f"${x:.2f}"


def build_report(results, ctx):
    now = datetime.now(timezone.utc).strftime("%d %b %Y %H:%M UTC")
    lines = [f"📊 Momentum Scan - {now}"]
    if ctx:
        lines.append("Market: " + " | ".join(ctx))
    lines.append("")
    if not results:
        lines.append("Ei mohurte filter pass kora kono coin pawa jayni.")
    for i, r in enumerate(results, 1):
        p, info = r["p"], r["info"]
        pc = p.get("priceChange") or {}
        mc = p.get("marketCap") or p.get("fdv")
        tech = []
        if "rsi" in info:
            tech.append(f"RSI {info['rsi']:.0f}")
        if "macd" in info:
            tech.append(f"MACD {info['macd']}")
        if info.get("breakout"):
            tech.append("Breakout")
        lines.append(f"{i}) {p['baseToken']['symbol']} ({p['chainId']}) - Score {r['score']:.0f}/100")
        lines.append(f"Price ${float(p.get('priceUsd') or 0):.8g} | 1h {pc.get('h1', 0):+.1f}% | 24h {pc.get('h24', 0):+.1f}%")
        lines.append(f"Liq {usd((p.get('liquidity') or {}).get('usd'))} | Vol24h {usd((p.get('volume') or {}).get('h24'))} | MC {usd(mc)}")
        if tech:
            lines.append(" | ".join(tech))
        for n in r["notes"][:4]:
            lines.append(f"✅ {n}")
        for f in r["flags"]:
            lines.append(f"⚠️ {f}")
        lines.append(p.get("url", ""))
        lines.append("")
    lines.append("⚠️ Eta financial advice na. Score = momentum + risk filter, pump-er guarantee na. DYOR.")
    return "\n".join(lines)


def send_telegram(text):
    if not TG_TOKEN or not TG_CHAT:
        log("Telegram token/chat id nei, console-e print korchi:")
        print(text)
        return False
    ok = True
    for i in range(0, len(text), 4000):
        r = S.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            data={"chat_id": TG_CHAT, "text": text[i:i + 4000], "disable_web_page_preview": True},
            timeout=20,
        )
        if not r.ok:
            log(f"Telegram error: {r.status_code} {r.text[:200]}")
            ok = False
    return ok


def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:  # noqa
        return {}


def save_state(st):
    with open(STATE_FILE, "w") as f:
        json.dump(st, f)


def maybe_tweet(results):
    if not POST_TWITTER or not results:
        return
    st = load_state()
    if time.time() - st.get("last_tweet", 0) < TWEET_EVERY_H * 3600:
        return
    top = [r for r in results if r["score"] >= TWEET_MIN_SCORE][:3]
    if not top:
        return
    names = ", ".join(f"{r['p']['baseToken']['symbol']} ({r['p']['chainId']})" for r in top)
    text = (f"🔎 Momentum watchlist: {names}\n"
            "Volume + buy pressure rising. Not financial advice, DYOR. High scam risk in new tokens.")
    try:
        import tweepy
        client = tweepy.Client(
            consumer_key=os.getenv("X_API_KEY"),
            consumer_secret=os.getenv("X_API_SECRET"),
            access_token=os.getenv("X_ACCESS_TOKEN"),
            access_token_secret=os.getenv("X_ACCESS_TOKEN_SECRET"),
        )
        client.create_tweet(text=text[:280])
        st["last_tweet"] = time.time()
        save_state(st)
        log("tweet posted")
    except Exception as e:  # noqa
        log(f"tweet failed: {e}")


def run_once():
    log("scan started")
    results = scan()
    report = build_report(results, chain_context())
    send_telegram(report)
    maybe_tweet(results)
    log("scan done")


if __name__ == "__main__":
    if "--test-telegram" in sys.argv:
        print("OK" if send_telegram("✅ Bot connected! Telegram setup thik ache.") else "FAILED - token/chat id check korun")
    elif "--once" in sys.argv:
        run_once()
    else:
        while True:
            try:
                run_once()
            except Exception as e:  # noqa
                log(f"error: {e}")
            time.sleep(SCAN_MINUTES * 60)
