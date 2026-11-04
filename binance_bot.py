#!/usr/bin/env python3
"""BINANCE GEMS Early Pump Radar — Binance Spot signals only; no order execution."""
from __future__ import annotations
import os, json, time, math, logging, statistics
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
import requests

BASES = [os.getenv("BINANCE_BASE_URL", "https://data-api.binance.vision"),
         "https://api.binance.com"]
QUOTE = "USDT"
TIMEFRAMES = {"15m": ("15m", 120), "1h": ("1h", 120), "4h": ("4h", 120)}
MIN_SCORE = int(os.getenv("MIN_SCORE", "65"))
STRONG_SCORE = int(os.getenv("STRONG_SCORE", "80"))
MAX_24H_CHANGE = float(os.getenv("MAX_24H_CHANGE_PCT", "15"))
VOLUME_RATIO_MIN = float(os.getenv("VOLUME_RATIO_MIN", "1.5"))
RISK_PCT = float(os.getenv("RISK_PER_TRADE_PCT", "0.75"))
CAPITAL_USD = float(os.getenv("REFERENCE_CAPITAL_USD", "1000"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))
STATE_PATH = Path("state.json")
DOCS = Path("docs")
DOCS.mkdir(exist_ok=True)
EXCLUDED_BASES = set("USDC FDUSD TUSD USDP DAI USDD BUSD EUR TRY USDE PYUSD USDS USTC".split())
LEVERAGED_SUFFIXES = ("UP", "DOWN", "BULL", "BEAR")
session = requests.Session()
session.headers.update({"User-Agent": "BINANCE-GEMS-Early-Pump-Radar/1.0"})

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def request_json(path: str, params: dict | None = None, timeout: int = 18):
    last = None
    for base in BASES:
        for attempt in range(3):
            try:
                r = session.get(base + path, params=params, timeout=timeout)
                if r.status_code in (418, 429):
                    time.sleep(2 ** (attempt + 1))
                    continue
                r.raise_for_status()
                return r.json()
            except Exception as exc:
                last = exc
                time.sleep(0.4 * (attempt + 1))
    raise RuntimeError(f"Binance request failed {path}: {last}")

def ema(values, period):
    if len(values) < period: return []
    k = 2 / (period + 1)
    out = [sum(values[:period]) / period]
    for v in values[period:]:
        out.append(v * k + out[-1] * (1-k))
    return [None] * (period-1) + out

def sma(values, period):
    if len(values) < period: return []
    return [None]*(period-1) + [sum(values[i-period+1:i+1])/period for i in range(period-1,len(values))]

def rsi(values, period=14):
    if len(values) <= period: return None
    gains=[]; losses=[]
    for a,b in zip(values[:-1], values[1:]):
        gains.append(max(b-a,0)); losses.append(max(a-b,0))
    ag=sum(gains[:period])/period; al=sum(losses[:period])/period
    for g,l in zip(gains[period:],losses[period:]):
        ag=(ag*(period-1)+g)/period; al=(al*(period-1)+l)/period
    if al == 0: return 100.0
    return 100 - 100/(1+ag/al)

def macd_hist(values):
    e12,e26=ema(values,12),ema(values,26)
    if not e26: return None
    line=[(a-b) if a is not None and b is not None else None for a,b in zip(e12,e26)]
    valid=[x for x in line if x is not None]
    sig=ema(valid,9)
    if not sig: return None
    return valid[-1]-sig[-1]

def candle_metrics(rows):
    # Binance kline: open time, open, high, low, close, volume, close time, quote volume, trades, taker base, taker quote...
    o=[float(x[1]) for x in rows]; h=[float(x[2]) for x in rows]
    l=[float(x[3]) for x in rows]; c=[float(x[4]) for x in rows]
    v=[float(x[5]) for x in rows]; q=[float(x[7]) for x in rows]
    e10,e20,e50,e200=ema(c,10),ema(c,20),ema(c,50),ema(c,200)
    rv=rsi(c)
    vm=sma(v,20)
    vr=(v[-1]/vm[-1]) if vm and vm[-1] else 0
    prevvr=(v[-2]/vm[-2]) if vm and len(vm)>1 and vm[-2] else 0
    body=abs(c[-1]-o[-1]); upper=h[-1]-max(c[-1],o[-1])
    lower=min(c[-1],o[-1])-l[-1]
    tr=[]
    for i in range(1,len(c)):
        tr.append(max(h[i]-l[i],abs(h[i]-c[i-1]),abs(l[i]-c[i-1])))
    atr=sum(tr[-14:])/max(1,min(14,len(tr)))
    obv=[0.0]
    for i in range(1,len(c)):
        obv.append(obv[-1]+(v[i] if c[i]>c[i-1] else -v[i] if c[i]<c[i-1] else 0))
    obv_rising=obv[-1] > obv[-6] if len(obv)>=6 else False
    avg_quote=sum(q[-20:])/max(1,len(q[-20:]))
    return {
        "open":o[-1],"high":h[-1],"low":l[-1],"close":c[-1],"volume":v[-1],
        "ema10":e10[-1] if e10 else None,"ema20":e20[-1] if e20 else None,
        "ema50":e50[-1] if e50 else None,"ema200":e200[-1] if e200 else None,
        "rsi":rv,"macd_hist":macd_hist(c),"volume_ratio":vr,"prev_volume_ratio":prevvr,
        "atr":atr,"upper_wick_body":upper/max(body,1e-12),"lower_wick":lower,
        "obv_rising":obv_rising,"avg_quote_volume":avg_quote,
        "higher_low":l[-1] > min(l[-6:-1]) if len(l)>=6 else False,
        "close_prev":c[-2],"high_prev":h[-2],"low_prev":l[-2],
        "recent_high":max(h[-21:-1]),"recent_low":min(l[-21:-1]),
        "green":c[-1]>o[-1],"close_above_prev_high":c[-1]>h[-2],
    }

def get_klines(symbol, interval, limit):
    return request_json("/api/v3/klines", {"symbol":symbol,"interval":interval,"limit":limit})

def market_caps():
    """CoinGecko primary (top 1000 by cap), CMC fallback; symbol collisions choose largest cap."""
    result={}
    try:
        for page in range(1,5):
            data=requests.get("https://api.coingecko.com/api/v3/coins/markets",
                params={"vs_currency":"usd","order":"market_cap_desc","per_page":250,"page":page,
                        "sparkline":"false"}, timeout=20).json()
            for coin in data:
                sym=str(coin.get("symbol","")).upper()
                cap=coin.get("market_cap")
                if sym and cap and (sym not in result or cap>result[sym]["market_cap"]):
                    result[sym]={"market_cap":float(cap),"name":coin.get("name"),"source":"CoinGecko"}
            time.sleep(0.25)
    except Exception as exc:
        logging.warning("CoinGecko market-cap lookup incomplete: %s", exc)
    key=os.getenv("COINMARKETCAP_API_KEY")
    if key:
        try:
            data=requests.get("https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest",
                params={"start":1,"limit":5000,"convert":"USD"},
                headers={"X-CMC_PRO_API_KEY":key},timeout=25).json().get("data",[])
            for coin in data:
                sym=str(coin.get("symbol","")).upper()
                cap=(coin.get("quote") or {}).get("USD",{}).get("market_cap")
                if sym and cap and (sym not in result or cap>result[sym]["market_cap"]):
                    result[sym]={"market_cap":float(cap),"name":coin.get("name"),"source":"CoinMarketCap"}
        except Exception as exc:
            logging.warning("CMC fallback failed: %s",exc)
    return result

def turnover_threshold(cap):
    if cap < 1_000_000: return 15.0
    if cap < 10_000_000: return 8.0
    if cap < 100_000_000: return 5.0
    if cap < 500_000_000: return 3.0
    return 2.0

def score_coin(symbol, ticker, caps, btc_regime):
    base=symbol[:-4]
    cap_data=caps.get(base)
    cap=cap_data["market_cap"] if cap_data else None
    quote_vol=float(ticker.get("quoteVolume",0) or 0)
    change=float(ticker.get("priceChangePercent",0) or 0)
    last=float(ticker.get("lastPrice",0) or 0)
    if last<=0 or change>MAX_24H_CHANGE: return None
    # Deep analysis across all eligible symbols, no arbitrary absolute quote-volume floor.
    try:
        with ThreadPoolExecutor(max_workers=3) as ex:
            f15=ex.submit(get_klines,symbol,"15m",120)
            f1h=ex.submit(get_klines,symbol,"1h",120)
            f4h=ex.submit(get_klines,symbol,"4h",120)
            m15=candle_metrics(f15.result()); m1h=candle_metrics(f1h.result()); m4h=candle_metrics(f4h.result())
    except Exception as exc:
        logging.debug("%s candle data unavailable: %s",symbol,exc); return None

    # 4H market structure (20)
    structure=0
    if m4h["close"] > (m4h["ema50"] or 0): structure+=7
    if m4h["ema20"] and m4h["ema50"] and m4h["ema20"]>m4h["ema50"]: structure+=6
    if m4h["higher_low"]: structure+=4
    if m1h["higher_low"]: structure+=3
    # Volume expansion (15)
    volume=min(15, max(0,(m15["volume_ratio"]-1.0)*10))
    if m15["volume_ratio"]>=VOLUME_RATIO_MIN: volume=max(volume,7)
    # EMA alignment (10)
    ema_score=0
    if m1h["close"]> (m1h["ema20"] or 0): ema_score+=4
    if m1h["ema10"] and m1h["ema20"] and m1h["ema10"]>m1h["ema20"]: ema_score+=3
    if m15["close"]>(m15["ema20"] or 0): ema_score+=3
    # RSI + MACD (10)
    momentum=0
    if m1h["rsi"] is not None and 50<=m1h["rsi"]<=67: momentum+=5
    if m15["rsi"] is not None and 52<=m15["rsi"]<=67: momentum+=3
    if m1h["macd_hist"] is not None and m1h["macd_hist"]>0: momentum+=2
    # Accumulation / OBV (10)
    accumulation=(6 if m1h["obv_rising"] else 0)+(4 if m15["obv_rising"] else 0)
    # Liquidity / order flow (10), relative spread proxy omitted unless bookTicker available
    liquidity=0
    if quote_vol>0: liquidity+=4
    taker_ratio=0.0
    # Binance kline taker-buy base volume is field 9; use latest candle proxy from fetched rows omitted here.
    if m15["volume_ratio"]>=1.5: liquidity+=3
    if m15["upper_wick_body"]<1.5: liquidity+=3
    # Market cap relative turnover (10)
    turnover=None; turnover_score=0
    if cap and cap>0:
        turnover=quote_vol/cap*100
        threshold=turnover_threshold(cap)
        turnover_score=10 if turnover>=threshold*1.5 else 7 if turnover>=threshold else 3 if turnover>=threshold*0.7 else 0
    # BTC regime (10)
    btc_score=10 if btc_regime else 0
    # Resistance room (5): distance to recent 1H high
    resistance_room=(m1h["recent_high"]-m1h["close"])/max(m1h["close"],1e-12)*100
    room_score=5 if resistance_room>=4 else 3 if resistance_room>=2 else 0
    parts={"structure":round(structure,1),"volume":round(volume,1),"ema":ema_score,
           "momentum":momentum,"accumulation":accumulation,"liquidity":liquidity,
           "turnover":turnover_score,"btc":btc_score,"resistance_room":room_score}
    score=round(sum(parts.values()),1)
    # Hard safety filters: don't chase stretched RSI or resistance; BTC weakness blocks strong signal.
    hard=[]
    if m1h["rsi"] is None or m1h["rsi"]>72: hard.append("RSI overextended/missing")
    if resistance_room<1.0: hard.append("Major resistance too close")
    if m15["volume_ratio"]<1.0: hard.append("No volume confirmation")
    if not btc_regime: hard.append("BTC regime weak")
    # Candidate status is separate from score: requires 15m trigger or a valid retest/reversal.
    trigger=(m15["close_above_prev_high"] and m15["green"] and m15["volume_ratio"]>=VOLUME_RATIO_MIN)
    pullback=(m15["green"] and m15["close"]>=(m15["ema20"] or 0) and m15["volume_ratio"]>=1.2)
    status="NO TRADE"
    if not hard and score>=STRONG_SCORE and (trigger or pullback): status="BUY CANDIDATE"
    elif score>=MIN_SCORE: status="WATCH"
    # Entry plan based on current 15m trigger candle high; stop below structure/ATR.
    entry=max(m15["high"],m15["close"]) if trigger else m15["close"]
    structural_low=min(m15["recent_low"],m15["low"])
    stop=min(structural_low,entry-1.5*max(m15["atr"],entry*0.005))
    if stop>=entry: stop=entry*0.97
    risk_dist=(entry-stop)/entry
    position_usd=(CAPITAL_USD*RISK_PCT/100)/risk_dist if risk_dist>0 else 0
    position_usd=min(position_usd,CAPITAL_USD*0.25)
    risk_reward_targets=[entry+(entry-stop)*2,entry+(entry-stop)*3,entry+(entry-stop)*4]
    return {"symbol":symbol,"base":base,"price":last,"change_24h":round(change,2),
        "quote_volume_24h":round(quote_vol,2),"market_cap":round(cap,2) if cap else None,
        "market_cap_source":cap_data["source"] if cap_data else None,
        "turnover_pct":round(turnover,2) if turnover is not None else None,
        "turnover_threshold_pct":turnover_threshold(cap) if cap else None,
        "score":score,"status":status,"hard_filters":hard,"score_breakdown":parts,
        "timeframes":{"15m":{"rsi":round(m15["rsi"],1) if m15["rsi"] else None,"volume_ratio":round(m15["volume_ratio"],2)},
                      "1h":{"rsi":round(m1h["rsi"],1) if m1h["rsi"] else None,"ema20":m1h["ema20"],"obv_rising":m1h["obv_rising"]},
                      "4h":{"ema50":m4h["ema50"],"higher_low":m4h["higher_low"]}},
        "entry":round(entry,12),"stop_loss":round(stop,12),
        "tp1":round(risk_reward_targets[0],12),"tp2":round(risk_reward_targets[1],12),
        "tp3":round(risk_reward_targets[2],12),"position_usd":round(position_usd,2),
        "risk_pct":RISK_PCT,"resistance_room_pct":round(resistance_room,2),
        "updated_at":datetime.now(timezone.utc).isoformat()}

def get_btc_regime():
    try:
        m=candle_metrics(get_klines("BTCUSDT","4h",220))
        return bool(m["close"]>(m["ema50"] or 0) and m["ema20"] and m["ema50"] and m["ema20"]>m["ema50"] and m["rsi"] and m["rsi"]>=48)
    except Exception as exc:
        logging.warning("BTC regime unavailable: %s",exc); return False

def send_telegram(text):
    token=os.getenv("TELEGRAM_BOT_TOKEN"); chat=os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat:
        logging.warning("Telegram secrets missing; skipping Telegram delivery."); return
    url=f"https://api.telegram.org/bot{token}/sendMessage"
    r=requests.post(url,json={"chat_id":chat,"text":text,"disable_web_page_preview":True},timeout=20)
    r.raise_for_status()

def load_state():
    try: return json.loads(STATE_PATH.read_text())
    except Exception: return {"sent":{},"scan_count":0}

def save_dashboard(results, scan_meta):
    payload={"updated_at":datetime.now(timezone.utc).isoformat(),"scan":scan_meta,
             "candidates":results[:150]}
    (DOCS/"data.json").write_text(json.dumps(payload,indent=2))
    return payload

def telegram_message(c):
    return (f"💎 BINANCE GEMS | EARLY PUMP CANDIDATE\n"
        f"#{c['symbol']}  | Score: {c['score']}/100\n"
        f"Entry: {c['entry']}\nSL: {c['stop_loss']}\n"
        f"TP1: {c['tp1']} | TP2: {c['tp2']} | TP3: {c['tp3']}\n"
        f"15m Volume: {c['timeframes']['15m']['volume_ratio']}x | "
        f"1h RSI: {c['timeframes']['1h']['rsi']}\n"
        f"24h Turnover/MC: {c['turnover_pct'] if c['turnover_pct'] is not None else 'N/A'}%\n"
        f"Position guide (reference capital): ${c['position_usd']}\n"
        f"Setup: 15m + 1h + 4h | Spot only\n"
        f"Confirmation required. Not an execution instruction.")

def main():
    started=time.time()
    logging.info("Loading Binance Spot universe...")
    info=request_json("/api/v3/exchangeInfo")
    valid=set()
    for s in info.get("symbols",[]):
        if s.get("status")!="TRADING" or s.get("quoteAsset")!=QUOTE or not s.get("isSpotTradingAllowed",False): continue
        base=s.get("baseAsset","")
        if base in EXCLUDED_BASES or base.endswith(LEVERAGED_SUFFIXES): continue
        valid.add(s["symbol"])
    tickers=request_json("/api/v3/ticker/24hr")
    ticker_map={x["symbol"]:x for x in tickers if x.get("symbol") in valid}
    caps=market_caps()
    btc=get_btc_regime()
    logging.info("Eligible pairs=%d; market-cap symbols=%d; BTC regime=%s",len(ticker_map),len(caps),btc)
    results=[]
    # Deep scan all eligible pairs; bounded concurrency to reduce API pressure.
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures={pool.submit(score_coin,s,t,caps,btc):s for s,t in ticker_map.items()}
        done=0
        for f in as_completed(futures):
            done+=1
            try:
                item=f.result()
                if item: results.append(item)
            except Exception as exc:
                logging.debug("Scan error %s: %s",futures[f],exc)
            if done%100==0: logging.info("Analyzed %d/%d pairs",done,len(futures))
    results.sort(key=lambda x:(x["score"],x["turnover_pct"] or 0),reverse=True)
    state=load_state(); now=int(time.time())
    # Send only newly qualifying signals; 24h cooldown per symbol, no pump/SL follow-up alerts.
    for c in results:
        if c["status"]!="BUY CANDIDATE": continue
        last=state["sent"].get(c["symbol"],0)
        if now-last < 86400: continue
        try:
            send_telegram(telegram_message(c))
            state["sent"][c["symbol"]]=now
            logging.info("Telegram signal sent: %s",c["symbol"])
            time.sleep(1.2)
        except Exception as exc: logging.error("Telegram send failed %s: %s",c["symbol"],exc)
    state["sent"]={k:v for k,v in state.get("sent",{}).items() if now-v<7*86400}
    state["scan_count"]=state.get("scan_count",0)+1
    state["last_scan"]=datetime.now(timezone.utc).isoformat()
    STATE_PATH.write_text(json.dumps(state,indent=2))
    eligible=[x for x in results if x["status"] in ("WATCH","BUY CANDIDATE")]
    meta={"eligible_pairs":len(ticker_map),"analyzed_pairs":len(results),"watch_count":sum(x["status"]=="WATCH" for x in results),
          "buy_candidates":sum(x["status"]=="BUY CANDIDATE" for x in results),"btc_bullish":btc,
          "duration_seconds":round(time.time()-started,1),"scan_count":state["scan_count"]}
    save_dashboard(eligible,meta)
    logging.info("Scan complete: %s",meta)

if __name__=="__main__":
    main()
