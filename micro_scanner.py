"""
Micro-cap early scanner (free, no API key) - Solana + BNB
Finds fresh coins with ~50k-2M market cap that show early momentum + safety checks.
Data: DexScreener (free), RugCheck (free, Solana). Report goes to Telegram.
"""
import os, json, time, requests

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")
CHAINS = {"solana", "bsc"}
MCAP_MIN, MCAP_MAX = 50_000, 2_000_000
LIQ_MIN = 12_000
AGE_MIN_H, AGE_MAX_H = 1, 24 * 7
MIN_SCORE = 60
SEEN_FILE = "seen.json"
DS = "https://api.dexscreener.com"
H = {"User-Agent": "Mozilla/5.0"}


def get(url, timeout=20):
    try:
        r = requests.get(url, headers=H, timeout=timeout)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print("ERR", url, e)
    return None


def candidates():
    """Collect token addresses from free DexScreener lists."""
    found = {}
    for path in ("/token-profiles/latest/v1", "/token-boosts/latest/v1", "/token-boosts/top/v1"):
        data = get(DS + path) or []
        for t in data:
            if t.get("chainId") in CHAINS and t.get("tokenAddress"):
                found[(t["chainId"], t["tokenAddress"])] = 1
    for q in ("solana new", "bsc new", "pump", "meme"):
        data = get(f"{DS}/latest/dex/search?q={q}") or {}
        for p in data.get("pairs", []):
            if p.get("chainId") in CHAINS:
                found[(p["chainId"], p["baseToken"]["address"])] = 1
    return list(found)


def best_pairs(cands):
    by_chain = {}
    for ch, addr in cands:
        by_chain.setdefault(ch, []).append(addr)
    out = []
    for ch, addrs in by_chain.items():
        for i in range(0, len(addrs), 30):
            data = get(f"{DS}/tokens/v1/{ch}/{','.join(addrs[i:i+30])}") or []
            best = {}
            for p in data:
                a = p["baseToken"]["address"]
                if a not in best or (p.get("liquidity") or {}).get("usd", 0) > (best[a].get("liquidity") or {}).get("usd", 0):
                    best[a] = p
            out += list(best.values())
            time.sleep(0.4)
    return out


def rugcheck(mint):
    d = get(f"https://api.rugcheck.xyz/v1/tokens/{mint}/report/summary")
    if not d:
        return None
    risks = [r.get("name", "") for r in d.get("risks", []) if r.get("level") in ("danger", "warn")]
    danger = [r.get("name", "") for r in d.get("risks", []) if r.get("level") == "danger"]
    return {"risks": risks, "danger": danger}


def score(p):
    s, why = 0, []
    liq = (p.get("liquidity") or {}).get("usd", 0)
    mcap = p.get("marketCap") or p.get("fdv") or 0
    vol = p.get("volume", {})
    tx = p.get("txns", {})
    pc = p.get("priceChange", {})
    # volume acceleration: last hour vs average hour of the day
    v1, v24 = vol.get("h1", 0), vol.get("h24", 0)
    if v24 > 0 and v1 > (v24 / 24) * 2:
        s += 25; why.append("volume spike x%.1f" % (v1 / (v24 / 24)))
    # buy pressure
    b, sl = tx.get("h1", {}).get("buys", 0), tx.get("h1", {}).get("sells", 0)
    if b + sl >= 30 and b / max(sl, 1) >= 1.3:
        s += 20; why.append(f"buy pressure {b}/{sl}")
    # liquidity depth vs mcap (healthy >= 8%)
    if mcap and liq / mcap >= 0.08:
        s += 15; why.append("liq/mcap %.0f%%" % (100 * liq / mcap))
    # early but not overextended
    if 2 <= pc.get("h1", 0) <= 60:
        s += 15; why.append("h1 %+.0f%%" % pc.get("h1", 0))
    if pc.get("h24", 0) < 400:
        s += 10
    else:
        why.append("already pumped")
    # activity
    if tx.get("h24", {}).get("buys", 0) + tx.get("h24", {}).get("sells", 0) >= 300:
        s += 10; why.append("active trading")
    if (p.get("info") or {}).get("socials"):
        s += 5; why.append("has socials")
    return s, why


def tg(text):
    if not TG_TOKEN:
        print(text); return
    requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                  json={"chat_id": TG_CHAT, "text": text, "disable_web_page_preview": True}, timeout=20)


def main():
    seen = {}
    if os.path.exists(SEEN_FILE):
        try: seen = json.load(open(SEEN_FILE))
        except Exception: seen = {}
    now = time.time()
    seen = {k: v for k, v in seen.items() if now - v < 86400 * 2}
    picks = []
    for p in best_pairs(candidates()):
        addr = p["baseToken"]["address"]
        mcap = p.get("marketCap") or p.get("fdv") or 0
        liq = (p.get("liquidity") or {}).get("usd", 0)
        created = p.get("pairCreatedAt")
        if not created or addr in seen:
            continue
        age_h = (now * 1000 - created) / 3.6e6
        if not (MCAP_MIN <= mcap <= MCAP_MAX and liq >= LIQ_MIN and AGE_MIN_H <= age_h <= AGE_MAX_H):
            continue
        sc, why = score(p)
        if sc < MIN_SCORE:
            continue
        rc = None
        if p["chainId"] == "solana":
            rc = rugcheck(addr)
            if rc and rc["danger"]:
                continue  # reject dangerous (mint/freeze authority, top holders, etc.)
        picks.append((sc, p, why, rc, age_h, mcap, liq))
    picks.sort(key=lambda x: -x[0])
    for sc, p, why, rc, age_h, mcap, liq in picks[:5]:
        seen[p["baseToken"]["address"]] = now
        warn = ("; ".join(rc["risks"][:3]) if rc and rc["risks"] else "none flagged" if rc else "n/a")
        tg(
            f"EARLY MICRO-CAP SIGNAL  score {sc}/100\n"
            f"{p['baseToken']['symbol']} ({p['chainId']})\n"
            f"MCap ${mcap:,.0f} | Liq ${liq:,.0f} | Age {age_h:.1f}h\n"
            f"Why: {', '.join(why)}\n"
            f"Rug warnings: {warn}\n"
            f"{p['url']}\n"
            f"Not financial advice. Micro-caps can go to zero. Use small size."
        )
    json.dump(seen, open(SEEN_FILE, "w"))
    print("done, signals:", len(picks))


if __name__ == "__main__":
    main()
