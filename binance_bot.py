#!/usr/bin/env python3
"""BINANCE GEMS — Binance Spot early-momentum scanner (signals only; no trading)."""
import os, time, json, argparse, logging, math
from datetime import datetime, timezone
from typing import Dict, List, Optional
import requests
import pandas as pd

LOG = logging.getLogger("binance_gems")
BASE_URLS = [os.getenv("BINANCE_BASE_URL", "https://data-api.binance.vision"),
             "https://api.binance.com"]
QUOTE = "USDT"
EXCLUDED = {"USDC","FDUSD","TUSD","USDP","DAI","USDD","BUSD","EUR","TRY","USDE","PYUSD"}
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "BINANCE-GEMS/1.0"})

def get_json(path: str, params=None):
    last = None
    for base in BASE_URLS:
        try:
            r = SESSION.get(base + path, params=params, timeout=12)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
    raise RuntimeError(f"Binance API unavailable: {last}")

def telegram_send(text: str):
    token, chat_id = os.getenv("TELEGRAM_BOT_TOKEN"), os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        LOG.info("Telegram credentials missing; signal printed only.")
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    r = SESSION.post(url, json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True}, timeout=15)
    r.raise_for_status()

def load_state(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f: return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError): return {"sent": {}}

def save_state(path: str, state: dict):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f: json.dump(state, f, indent=2)
    os.replace(tmp, path)

def get_universe() -> List[str]:
    info = get_json("/api/v3/exchangeInfo")
    symbols = []
    for s in info.get("symbols", []):
        if (s.get("status") == "TRADING" and s.get("isSpotTradingAllowed", True)
            and s.get("quoteAsset") == QUOTE and s.get("baseAsset") not in EXCLUDED
            and not any(s.get("baseAsset","").endswith(x) for x in ("UP","DOWN","BULL","BEAR"))):
            symbols.append(s["symbol"])
    return symbols

def klines(symbol: str, interval: str, limit: int) -> pd.DataFrame:
    raw = get_json("/api/v3/klines", {"symbol": symbol, "interval": interval, "limit": limit})
    cols = ["open_time","open","high","low","close","volume","close_time","quote_volume",
            "trades","taker_buy_base","taker_buy_quote","ignore"]
    df = pd.DataFrame(raw, columns=cols)
    for c in ["open","high","low","close","volume","quote_volume","taker_buy_base","taker_buy_quote"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def ema(s, n): return s.ewm(span=n, adjust=False).mean()
def rsi(s, n=14):
    d=s.diff(); up=d.clip(lower=0).ewm(alpha=1/n, adjust=False).mean()
    dn=(-d.clip(upper=0)).ewm(alpha=1/n, adjust=False).mean()
    rs=up/dn.replace(0, float("nan"))
    return 100-(100/(1+rs))

def analyze(symbol: str, ticker: dict) -> Optional[dict]:
    # Closed 1h and 15m candles only; exclude the currently-forming candle.
    h = klines(symbol, "1h", 121).iloc[:-1].copy()
    m = klines(symbol, "15m", 101).iloc[:-1].copy()
    if len(h) < 60 or len(m) < 30: return None

    h["ema20"], h["ema50"] = ema(h.close,20), ema(h.close,50)
    h["rsi"] = rsi(h.close)
    m["ema20"], m["rsi"] = ema(m.close,20), rsi(m.close)
    m["vol_avg"] = m.volume.shift(1).rolling(20).mean()
    m["atr"] = (m.high-m.low).rolling(14).mean()
    last, prev = m.iloc[-1], m.iloc[-2]
    ht = h.iloc[-1]
    price = float(ticker["lastPrice"])
    if not all(pd.notna(x) for x in [last.rsi, last.vol_avg, last.atr, ht.rsi]): return None

    change = float(ticker.get("priceChangePercent", 0))
    vol_ratio = float(last.volume / last.vol_avg) if last.vol_avg else 0
    prev_avg = m.volume.shift(1).rolling(20).mean().iloc[-2]
    prev_ratio = float(prev.volume / prev_avg) if pd.notna(prev_avg) and prev_avg else 0
    ema_dist = float(last.close / last.ema20) if last.ema20 else 99
    candle_range = max(float(last.high-last.low), 1e-12)
    body = abs(float(last.close-last.open))
    upper_wick = float(last.high-max(last.open,last.close))
    wick_body = upper_wick / max(body, 1e-12)
    body_atr = body / max(float(last.atr), 1e-12)
    taker_buy_ratio = float(last.taker_buy_quote / max(last.quote_volume, 1e-12))

    # Score is intentionally transparent; no fixed 24h quote-volume floor.
    score = 0
    reasons = []
    if ht.close > ht.ema50 and ht.ema20 > ht.ema50:
        score += 12; reasons.append("1H trend bullish")
    if 52 <= float(last.rsi) <= 67:
        score += 8; reasons.append(f"15M RSI {last.rsi:.1f}")
    if 48 <= float(ht.rsi) <= 75:
        score += 4; reasons.append(f"1H RSI {ht.rsi:.1f}")
    if vol_ratio >= 1.5:
        score += 10; reasons.append(f"Volume {vol_ratio:.2f}x")
    if prev_ratio >= 1.2:
        score += 4; reasons.append(f"Previous volume {prev_ratio:.2f}x")
    if 1.00 <= ema_dist <= 1.06:
        score += 6; reasons.append("Price holding near EMA20")
    if body_atr >= 0.4:
        score += 4; reasons.append("Candle body confirmed")
    if wick_body <= 1.5:
        score += 3; reasons.append("Upper wick controlled")
    if taker_buy_ratio >= 0.55:
        score += 4; reasons.append(f"Taker buying {taker_buy_ratio*100:.0f}%")
    if -2 <= change <= 15:
        score += 3; reasons.append(f"24H change {change:.2f}%")
    # Avoid chasing a stretched move and avoid weak 15m close.
    if last.close <= last.open or price < float(last.ema20): return None
    if change > float(os.getenv("MAX_24H_CHANGE_PCT","15")): return None
    if vol_ratio < 1.5 or not (52 <= float(last.rsi) <= 67): return None
    if ema_dist > 1.06 or wick_body > 1.5 or body_atr < 0.4: return None
    if score < int(os.getenv("MIN_SCORE","45")): return None

    # Basic nearby-resistance guard: reject if within 1.5% of recent 15m swing high.
    swing_high = float(m.high.iloc[-21:-1].max())
    if swing_high > price and (swing_high-price)/price < 0.015: return None

    # Illustrative risk levels; not a promise or trading instruction.
    atr = float(last.atr)
    entry = price
    stop = max(0.0, entry - 1.5*atr)
    tp = entry + 2.0*(entry-stop)
    return {"symbol":symbol, "price":price, "score":score, "reasons":reasons,
            "rsi":float(last.rsi), "volume_ratio":vol_ratio, "change":change,
            "entry":entry, "stop":stop, "tp":tp}

def fmt_signal(item: dict, serial: int) -> str:
    why = " • ".join(item["reasons"][:5])
    return (f"💎 BINANCE GEMS | SPOT SIGNAL #{serial}\n"
            f"🪙 {item['symbol']}\n"
            f"⭐ Score: {item['score']}/58\n"
            f"Entry area: {item['entry']:.8g}\n"
            f"TP reference: {item['tp']:.8g}\n"
            f"Risk reference: {item['stop']:.8g}\n"
            f"RSI: {item['rsi']:.1f} | Volume: {item['volume_ratio']:.2f}x | 24H: {item['change']:.2f}%\n"
            f"Why: {why}\n"
            f"⚠️ Research signal only. No guaranteed returns; confirm support/resistance and liquidity.")

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--watch", type=int, default=12)
    parser.add_argument("--force", action="store_true")
    args=parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    state_path=os.getenv("STATE_FILE","state_binance.json")
    state=load_state(state_path)
    tickers=get_json("/api/v3/ticker/24hr")
    ticker_map={x["symbol"]:x for x in tickers}
    universe=[s for s in get_universe() if s in ticker_map]
    LOG.info("Scanning %d Binance Spot USDT pairs",len(universe))
    found=[]
    for i,symbol in enumerate(universe):
        try:
            item=analyze(symbol,ticker_map[symbol])
            if item: found.append(item)
        except Exception as e:
            LOG.debug("%s: %s",symbol,e)
        if (i+1)%100==0: LOG.info("Scanned %d/%d",i+1,len(universe))
        time.sleep(0.035)  # be gentle with public API rate limits
    found.sort(key=lambda x:x["score"],reverse=True)
    sent=state.setdefault("sent",{})
    serial=int(state.get("serial",0))
    posted=0
    for item in found[:max(1,args.watch)]:
        sym=item["symbol"]
        # One alert per symbol per UTC day; --force bypasses duplicate protection.
        today=datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if not args.force and sent.get(sym)==today: continue
        serial+=1
        message=fmt_signal(item,serial)
        print("\n"+message+"\n")
        telegram_send(message)
        sent[sym]=today
        posted+=1
        time.sleep(2)
    state["serial"]=serial
    save_state(state_path,state)
    LOG.info("Qualified=%d, posted=%d",len(found),posted)

if __name__=="__main__": main()
