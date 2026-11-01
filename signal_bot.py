#!/usr/bin/env python3
"""
Multi-layer crypto futures signal bot -> Telegram.

Design follows the public description in AminoidAgent/Aminoid-Alpha (docs only,
no source code was published there), re-implemented from scratch:

  * 5 analytical layers (Trend 4H, Reversal 1H, Momentum 1H, Entry timing 15m,
    Microstructure) + cross-cutting inputs (ADX, relative strength vs BTC)
  * independent LONG / SHORT scoring out of 11.0
  * multi-condition gate: min score, layer participation, trend-or-reversal
    route, disagreement cap
  * BTC shock flag, price-aware dedup/cooldown
  * every number is deterministic - no AI / paid API needed

Ichimoku / MFI / OBV logic and the Telegram chunk+retry sending are ported from
cryptosignal/crypto-signal (original needs TA-Lib and very old pandas; this
version is pure numpy/pandas/requests).

All weights and thresholds below are MY OWN choices. They are NOT the
original author's (those are private) and they have NOT been backtested.

Usage:
  python signal_bot.py --test       send a "bot connected" message
  python signal_bot.py --scan       scan watchlist, send new signals (default)
  python signal_bot.py --dry-run    scan, print signals, send nothing
  python signal_bot.py --selftest   offline check on synthetic data
"""
import argparse
import html
import json
import logging
import os
import sys
import time
import zlib
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger("signals")


# --------------------------------------------------------------------------
# Config (override with environment variables / GitHub Actions env)
# --------------------------------------------------------------------------
def _num(name, default):
    return float(os.getenv(name, default))


MIN_SCORE = _num("MIN_SCORE", 6.0)          # winning side score (max 11.0)
MIN_LAYERS = int(_num("MIN_LAYERS", 3))     # layers (of 5) that must confirm
MAX_OPP_RATIO = _num("MAX_OPP_RATIO", 0.40)  # losing side <= ratio * winning side
MIN_MARGIN = _num("MIN_MARGIN", 2.5)        # winning - losing score
MIN_RR = _num("MIN_RR", 1.2)                # min reward/risk to TP1
COOLDOWN_H = _num("COOLDOWN_H", 3.0)        # min hours between same coin/direction
HARD_EXPIRY_H = _num("HARD_EXPIRY_H", 24.0)  # after this, repeat allowed regardless of price
MAX_SIGNALS = int(_num("MAX_SIGNALS", 4))   # max messages per run
SHOCK_PCT = _num("SHOCK_PCT", 0.018)        # BTC move over ~1h that counts as a shock
WORKERS = int(_num("WORKERS", 6))
DATA_SOURCE = os.getenv("DATA_SOURCE", "auto").lower()  # auto | binance | gate
STATE_FILE = os.getenv("STATE_FILE", "state.json")
REQUIRE_ROUTE = True  # selftest switches this off

# (coin, tier)  tier 1 = majors, 2 = established, 3 = smaller / thinner
WATCHLIST = [
    ("BTC", 1), ("ETH", 1), ("BNB", 1), ("SOL", 1), ("XRP", 1),
    ("ADA", 2), ("DOGE", 2), ("AVAX", 2), ("LINK", 2), ("DOT", 2),
    ("LTC", 2), ("BCH", 2), ("ATOM", 2), ("NEAR", 2), ("TRX", 2),
    ("UNI", 2), ("AAVE", 2), ("ETC", 2), ("FIL", 2), ("APT", 2),
    ("ARB", 2), ("OP", 2), ("SUI", 2), ("HBAR", 2),
    ("INJ", 3), ("TIA", 3), ("SEI", 3), ("WLD", 3), ("PEPE", 3),
    ("RUNE", 3), ("TON", 3), ("ENA", 3),
]

# --------------------------------------------------------------------------
# Indicator weights per layer. Per side the weights add up to 11.0.
#   layer 1 Trend(4H) 2 Reversal(1H) 3 Momentum(1H) 4 Timing(15m)
#         5 Microstructure   0 cross-cutting
# --------------------------------------------------------------------------
W = {
    "ema200": 0.75, "macd4h": 0.75, "structure": 0.5, "ichimoku": 0.75,
    "rsi_ext": 0.5, "rsi_div": 0.75, "bb_bounce": 0.3, "bb_break": 0.3,
    "candle": 0.4, "sr_prox": 0.5, "mfi": 0.4,
    "vol_surge": 0.75, "macd1h": 0.5, "stoch_cross": 0.5, "vwap": 0.35, "obv": 0.4,
    "burst": 0.2, "fast_rsi": 0.2, "body": 0.2, "streak": 0.2,
    "funding": 0.4, "oi": 0.4,
    "adx": 0.5, "rs_btc": 0.5,
}
LAYER = {
    "ema200": 1, "macd4h": 1, "structure": 1, "ichimoku": 1,
    "rsi_ext": 2, "rsi_div": 2, "bb_bounce": 2, "bb_break": 2, "candle": 2, "sr_prox": 2, "mfi": 2,
    "vol_surge": 3, "macd1h": 3, "stoch_cross": 3, "vwap": 3, "obv": 3,
    "burst": 4, "fast_rsi": 4, "body": 4, "streak": 4,
    "funding": 5, "oi": 5,
    "adx": 0, "rs_btc": 0,
}
LAYER_NAMES = {1: "Trend", 2: "Reversal", 3: "Momentum", 4: "Timing", 5: "Microstructure"}
REVERSAL_CLUSTER = {"rsi_div", "sr_prox", "stoch_cross", "fast_rsi", "rsi_ext", "candle", "mfi"}

Vote = namedtuple("Vote", "name layer side w text")


# --------------------------------------------------------------------------
# Indicator math (numpy / pandas only)
# --------------------------------------------------------------------------
def ema(s, n):
    return s.ewm(span=n, adjust=False).mean()


def rsi(s, n=14):
    d = s.diff()
    up = d.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean()
    rs = up / dn.replace(0, np.nan)
    out = 100 - 100 / (1 + rs)
    flat = np.where(up > 0, 100.0, 50.0)
    out = out.where(dn != 0, pd.Series(flat, index=s.index))
    return out.fillna(50.0)


def macd_hist(s):
    m = ema(s, 12) - ema(s, 26)
    return m - ema(m, 9)


def atr(df, n=14):
    pc = df.c.shift(1)
    tr = pd.concat([df.h - df.l, (df.h - pc).abs(), (df.l - pc).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / n, adjust=False).mean()


def adx(df, n=14):
    up = df.h.diff()
    dn = -df.l.diff()
    plus = pd.Series(np.where((up > dn) & (up > 0), up, 0.0), index=df.index)
    minus = pd.Series(np.where((dn > up) & (dn > 0), dn, 0.0), index=df.index)
    a = atr(df, n).replace(0, np.nan)
    pdi = 100 * plus.ewm(alpha=1 / n, adjust=False).mean() / a
    mdi = 100 * minus.ewm(alpha=1 / n, adjust=False).mean() / a
    dx = 100 * (pdi - mdi).abs() / (pdi + mdi).replace(0, np.nan)
    return (dx.ewm(alpha=1 / n, adjust=False).mean().fillna(0),
            pdi.fillna(0), mdi.fillna(0))


def stoch_rsi(s, n=14, k=3, d=3):
    r = rsi(s, n)
    lo, hi = r.rolling(n).min(), r.rolling(n).max()
    st = (r - lo) / (hi - lo).replace(0, np.nan)
    kk = st.rolling(k).mean()
    return kk, kk.rolling(d).mean()


def mfi(df, n=14):
    """Money Flow Index (ported from cryptosignal/crypto-signal, TA-Lib free)."""
    tp = (df.h + df.l + df.c) / 3
    mf = tp * df.v
    d = tp.diff()
    pos = mf.where(d > 0, 0.0).rolling(n).sum()
    neg = mf.where(d < 0, 0.0).rolling(n).sum()
    out = 100 - 100 / (1 + pos / neg.replace(0, np.nan))
    out = out.where(neg != 0, pd.Series(np.where(pos > 0, 100.0, 50.0), index=df.index))
    return out.fillna(50.0)


def obv(df):
    """On-Balance Volume (ported from cryptosignal/crypto-signal)."""
    return (np.sign(df.c.diff().fillna(0)) * df.v).cumsum()


def ichimoku_cloud(df):
    """Tenkan/Kijun/Span A/Span B with the repo's rules: hot = spanA>spanB and
    close>spanA, cold = spanA<spanB and close<spanA (no 26-bar displacement)."""
    def mid(n):
        return (df.h.rolling(n).max() + df.l.rolling(n).min()) / 2
    ten, kij, span_b = mid(9), mid(26), mid(52)
    span_a = (ten + kij) / 2
    return span_a, span_b


def swings(df, k=3):
    """Fractal swing highs / lows -> ([(idx, price)], [(idx, price)])."""
    h, l = df.h.values, df.l.values
    highs, lows = [], []
    for i in range(k, len(df) - k):
        wh, wl = h[i - k:i + k + 1], l[i - k:i + k + 1]
        if h[i] == wh.max() and (wh == h[i]).sum() == 1:
            highs.append((i, float(h[i])))
        if l[i] == wl.min() and (wl == l[i]).sum() == 1:
            lows.append((i, float(l[i])))
    return highs, lows


# --------------------------------------------------------------------------
# Exchange data (public endpoints, no API key needed)
# --------------------------------------------------------------------------
def make_session():
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=0.7,
                  status_forcelist=(429, 500, 502, 503, 504), allowed_methods=("GET",))
    s.mount("https://", HTTPAdapter(max_retries=retry, pool_maxsize=16))
    s.headers["User-Agent"] = "signal-bot/1.0"
    return s


class Binance:
    name = "binance"
    base = "https://fapi.binance.com"

    def __init__(self, session):
        self.s = session

    def sym(self, coin):
        return {"PEPE": "1000PEPEUSDT"}.get(coin, coin + "USDT")

    def _get(self, path, **params):
        r = self.s.get(self.base + path, params=params, timeout=15)
        r.raise_for_status()
        return r.json()

    def klines(self, coin, interval, limit):
        d = self._get("/fapi/v1/klines", symbol=self.sym(coin), interval=interval, limit=limit)
        df = pd.DataFrame(d).iloc[:, :6]
        df.columns = ["t", "o", "h", "l", "c", "v"]
        return df.astype(float)

    def funding(self, coin):
        d = self._get("/fapi/v1/fundingRate", symbol=self.sym(coin), limit=6)
        return [float(x["fundingRate"]) for x in d]

    def oi(self, coin):
        d = self._get("/futures/data/openInterestHist", symbol=self.sym(coin), period="1h", limit=6)
        return [float(x["sumOpenInterest"]) for x in d]


class Gate:
    name = "gate"
    base = "https://api.gateio.ws/api/v4"

    def __init__(self, session):
        self.s = session

    def sym(self, coin):
        return coin + "_USDT"

    def _get(self, path, **params):
        r = self.s.get(self.base + path, params=params, timeout=15)
        r.raise_for_status()
        return r.json()

    def klines(self, coin, interval, limit):
        d = self._get("/futures/usdt/candlesticks", contract=self.sym(coin),
                      interval=interval, limit=limit)
        rows = [[x["t"], x["o"], x["h"], x["l"], x["c"], x["v"]] for x in d]
        df = pd.DataFrame(rows, columns=["t", "o", "h", "l", "c", "v"]).astype(float)
        return df.sort_values("t").reset_index(drop=True)

    def funding(self, coin):
        d = self._get("/futures/usdt/funding_rate", contract=self.sym(coin), limit=6)
        d = sorted(d, key=lambda x: x["t"])
        return [float(x["r"]) for x in d]

    def oi(self, coin):
        d = self._get("/futures/usdt/contract_stats", contract=self.sym(coin),
                      interval="1h", limit=6)
        d = sorted(d, key=lambda x: x.get("time", 0))
        return [float(x.get("open_interest_usd") or x["open_interest"]) for x in d]


def pick_exchange(session):
    order = {"binance": [Binance], "gate": [Gate]}.get(DATA_SOURCE, [Binance, Gate])
    for cls in order:
        ex = cls(session)
        try:
            ex.klines("BTC", "15m", 5)
            log.info("data source: %s", ex.name)
            return ex
        except Exception as e:  # blocked region (HTTP 451/403), outage, ...
            log.warning("%s unavailable: %s", cls.name, e)
    raise SystemExit("No exchange data source reachable")


# --------------------------------------------------------------------------
# Evaluation: every indicator votes LONG ("L") or SHORT ("S") independently
# --------------------------------------------------------------------------
def evaluate(coin, price, d15, d1h, d4h, fund, oi, btc):
    votes = []

    def vote(name, side, text):
        votes.append(Vote(name, LAYER[name], side, W[name], text))

    c4, c1, c15 = d4h.c, d1h.c, d15.c
    atr1 = float(atr(d1h).iloc[-1])
    atr15 = float(atr(d15).iloc[-1])
    if not (atr1 > 0 and atr15 > 0):
        return None

    # ---- Layer 1: Trend (4H) ----
    n = max(20, min(200, len(d4h) - 10))
    e = float(ema(c4, n).iloc[-1])
    dist = (price - e) / e
    if dist > 0.003:
        vote("ema200", "L", f"price above 4H EMA{n}")
    elif dist < -0.003:
        vote("ema200", "S", f"price below 4H EMA{n}")

    h4 = macd_hist(c4)
    if h4.iloc[-1] > 0 and h4.iloc[-1] > h4.iloc[-2]:
        vote("macd4h", "L", "4H MACD momentum building up")
    elif h4.iloc[-1] < 0 and h4.iloc[-1] < h4.iloc[-2]:
        vote("macd4h", "S", "4H MACD momentum building down")

    sa, sb_ = ichimoku_cloud(d4h)
    if not (np.isnan(sa.iloc[-1]) or np.isnan(sb_.iloc[-1])):
        if sa.iloc[-1] > sb_.iloc[-1] and price > sa.iloc[-1]:
            vote("ichimoku", "L", "4H Ichimoku bullish (price above bullish cloud)")
        elif sa.iloc[-1] < sb_.iloc[-1] and price < sa.iloc[-1]:
            vote("ichimoku", "S", "4H Ichimoku bearish (price below bearish cloud)")

    sh, sl = swings(d4h.tail(90).reset_index(drop=True), 2)
    if len(sh) >= 2 and len(sl) >= 2:
        if sh[-1][1] > sh[-2][1] and sl[-1][1] > sl[-2][1]:
            vote("structure", "L", "4H structure: higher highs and higher lows")
        elif sh[-1][1] < sh[-2][1] and sl[-1][1] < sl[-2][1]:
            vote("structure", "S", "4H structure: lower highs and lower lows")

    # ---- Layer 2: Reversal (1H) ----
    r1 = rsi(c1)
    rv = float(r1.iloc[-1])
    if rv < 30:
        vote("rsi_ext", "L", f"1H RSI oversold ({rv:.0f})")
    elif rv > 70:
        vote("rsi_ext", "S", f"1H RSI overbought ({rv:.0f})")

    mf = float(mfi(d1h).iloc[-1])
    if mf < 20:
        vote("mfi", "L", f"1H money flow oversold (MFI {mf:.0f})")
    elif mf > 80:
        vote("mfi", "S", f"1H money flow overbought (MFI {mf:.0f})")

    sub = d1h.tail(60).reset_index(drop=True)
    rs_ = r1.tail(60).reset_index(drop=True).values
    dh, dl = swings(sub, 2)
    last = len(sub) - 1
    if len(dl) >= 2:
        (i1, p1), (i2, p2) = dl[-2], dl[-1]
        if p2 < p1 and rs_[i2] > rs_[i1] and i2 - i1 >= 4 and last - i2 <= 10:
            vote("rsi_div", "L", "bullish RSI divergence on 1H")
    if len(dh) >= 2:
        (i1, p1), (i2, p2) = dh[-2], dh[-1]
        if p2 > p1 and rs_[i2] < rs_[i1] and i2 - i1 >= 4 and last - i2 <= 10:
            vote("rsi_div", "S", "bearish RSI divergence on 1H")

    mid = c1.rolling(20).mean()
    sd = c1.rolling(20).std()
    up_b, lo_b = mid + 2 * sd, mid - 2 * sd
    width = (up_b - lo_b) / mid
    wq = width.rolling(100, min_periods=50).quantile(0.2)
    squeezed = bool((width.iloc[-7:-1] <= wq.iloc[-7:-1]).any())
    if squeezed:
        if c1.iloc[-1] > up_b.iloc[-1]:
            vote("bb_break", "L", "Bollinger squeeze broke out upward")
        elif c1.iloc[-1] < lo_b.iloc[-1]:
            vote("bb_break", "S", "Bollinger squeeze broke out downward")
        elif d1h.l.iloc[-1] <= lo_b.iloc[-1] < c1.iloc[-1]:
            vote("bb_bounce", "L", "bounce off lower Bollinger band after squeeze")
        elif d1h.h.iloc[-1] >= up_b.iloc[-1] > c1.iloc[-1]:
            vote("bb_bounce", "S", "rejection at upper Bollinger band after squeeze")

    o, h_, l_, c_ = (float(d1h[k].iloc[-1]) for k in ("o", "h", "l", "c"))
    po, pc = float(d1h.o.iloc[-2]), float(d1h.c.iloc[-2])
    body, rng = abs(c_ - o), h_ - l_
    if rng > 0:
        lower, upper = min(o, c_) - l_, h_ - max(o, c_)
        down_ctx = c1.iloc[-2] < c1.iloc[-5]
        up_ctx = c1.iloc[-2] > c1.iloc[-5]
        if lower >= 2 * max(body, 1e-12) and upper <= 0.35 * rng and down_ctx:
            vote("candle", "L", "hammer candle on 1H")
        elif upper >= 2 * max(body, 1e-12) and lower <= 0.35 * rng and up_ctx:
            vote("candle", "S", "shooting-star candle on 1H")
        elif pc < po and c_ > o and c_ >= po and o <= pc:
            vote("candle", "L", "bullish engulfing candle on 1H")
        elif pc > po and c_ < o and c_ <= po and o >= pc:
            vote("candle", "S", "bearish engulfing candle on 1H")

    sup, res = [], []
    for df_, lab, k in ((d1h.tail(150).reset_index(drop=True), "1H", 3),
                        (d4h.tail(100).reset_index(drop=True), "4H", 3)):
        hh, ll = swings(df_, k)
        sup += [(p, f"{lab} support") for _, p in ll]
        res += [(p, f"{lab} resistance") for _, p in hh]
    below = [p for p, _ in sup if p <= price]
    above = [p for p, _ in res if p >= price]
    ds = price - max(below) if below else None
    dr = min(above) - price if above else None
    near_s = ds is not None and ds <= 0.6 * atr1
    near_r = dr is not None and dr <= 0.6 * atr1
    if near_s and (not near_r or ds <= dr):
        vote("sr_prox", "L", "price sitting on nearby support")
    elif near_r:
        vote("sr_prox", "S", "price pressing into nearby resistance")

    # ---- Layer 3: Momentum (1H) ----
    v = d1h.v
    avg_v = float(v.iloc[-21:-1].mean())
    if avg_v > 0 and v.iloc[-1] >= 1.8 * avg_v:
        if c_ > o:
            vote("vol_surge", "L", "volume surge on a green 1H candle")
        elif c_ < o:
            vote("vol_surge", "S", "volume surge on a red 1H candle")

    h1 = macd_hist(c1)
    if h1.iloc[-1] > 0 and h1.iloc[-1] > h1.iloc[-2]:
        vote("macd1h", "L", "1H MACD momentum rising")
    elif h1.iloc[-1] < 0 and h1.iloc[-1] < h1.iloc[-2]:
        vote("macd1h", "S", "1H MACD momentum falling")

    k_, d_ = stoch_rsi(c1)
    if not (k_.iloc[-3:].isna().any() or d_.iloc[-3:].isna().any()):
        up_x = (k_.iloc[-2] <= d_.iloc[-2] and k_.iloc[-1] > d_.iloc[-1]) or \
               (k_.iloc[-3] <= d_.iloc[-3] and k_.iloc[-2] > d_.iloc[-2])
        dn_x = (k_.iloc[-2] >= d_.iloc[-2] and k_.iloc[-1] < d_.iloc[-1]) or \
               (k_.iloc[-3] >= d_.iloc[-3] and k_.iloc[-2] < d_.iloc[-2])
        if up_x and k_.iloc[-1] < 0.5:
            vote("stoch_cross", "L", "bullish stochastic RSI cross")
        elif dn_x and k_.iloc[-1] > 0.5:
            vote("stoch_cross", "S", "bearish stochastic RSI cross")

    ob = obv(d1h)
    ob_ema = ema(ob, 20)
    if ob.iloc[-1] > ob_ema.iloc[-1] and ob.iloc[-1] > ob.iloc[-5]:
        vote("obv", "L", "1H on-balance volume rising (accumulation)")
    elif ob.iloc[-1] < ob_ema.iloc[-1] and ob.iloc[-1] < ob.iloc[-5]:
        vote("obv", "S", "1H on-balance volume falling (distribution)")

    tp = (d1h.h + d1h.l + d1h.c) / 3
    vw =float((tp * v).tail(24).sum() / max(v.tail(24).sum(), 1e-12))
    dev = (price - vw) / vw
    if dev > 0.002:
        vote("vwap", "L", "price above 24h VWAP")
    elif dev < -0.002:
        vote("vwap", "S", "price below 24h VWAP")

    # ---- Layer 4: Entry timing (15m) ----
    last3 = d15.tail(3)
    if (last3.c > last3.o).all() and d15.c.iloc[-1] - d15.o.iloc[-3] >= atr15:
        vote("burst", "L", "15m run of 3 strong green candles")
    elif (last3.c < last3.o).all() and d15.o.iloc[-3] - d15.c.iloc[-1] >= atr15:
        vote("burst", "S", "15m run of 3 strong red candles")

    r7 = rsi(c15, 7)
    a, b, c_r = float(r7.iloc[-1]), float(r7.iloc[-2]), float(r7.iloc[-3])
    up_f = (b <= 50 < a) or (c_r <= 50 < b) or (b < 30 <= a)
    dn_f = (b >= 50 > a) or (c_r >= 50 > b) or (b > 70 >= a)
    if up_f and not dn_f:
        vote("fast_rsi", "L", "fast 15m RSI crossed up")
    elif dn_f and not up_f:
        vote("fast_rsi", "S", "fast 15m RSI crossed down")

    lo15, hi15 = float(d15.o.iloc[-1]), float(d15.c.iloc[-1])
    r15 = float(d15.h.iloc[-1] - d15.l.iloc[-1])
    if r15 > 0 and abs(hi15 - lo15) / r15 >= 0.65 and abs(hi15 - lo15) >= 0.6 * atr15:
        vote("body", "L" if hi15 > lo15 else "S", "strong-bodied 15m candle")

    streak, sdir = 0, 0
    for i in range(len(d15) - 1, -1, -1):
        dd = 1 if d15.c.iloc[i] > d15.o.iloc[i] else -1 if d15.c.iloc[i] < d15.o.iloc[i] else 0
        if dd == 0 or (sdir and dd != sdir):
            break
        sdir = dd
        streak += 1
    if streak >= 4:
        vote("streak", "L" if sdir > 0 else "S", f"{streak} straight {'green' if sdir > 0 else 'red'} 15m candles")

    # ---- Layer 5: Microstructure ----
    if fund:
        f = fund[-1]
        if f <= -0.0001:
            vote("funding", "L", "negative funding (shorts crowded)")
        elif f >= 0.0003:
            vote("funding", "S", "very positive funding (longs crowded)")
    if oi and len(oi) >= 5 and oi[-5] > 0:
        oi_chg = oi[-1] / oi[-5] - 1
        px_chg = float(c1.iloc[-1] / c1.iloc[-5] - 1)
        if oi_chg >= 0.02:
            if px_chg >= 0.004:
                vote("oi", "L", "open interest rising with price (fresh longs)")
            elif px_chg <= -0.004:
                vote("oi", "S", "open interest rising as price falls (fresh shorts)")

    # ---- Cross-cutting ----
    adx_s, pdi, mdi = adx(d1h)
    adx_v = float(adx_s.iloc[-1])
    if adx_v >= 22:
        vote("adx", "L" if pdi.iloc[-1] > mdi.iloc[-1] else "S",
             f"1H ADX {adx_v:.0f}, {'buyers' if pdi.iloc[-1] > mdi.iloc[-1] else 'sellers'} in control")
    if coin != "BTC" and btc.get("ret12") is not None:
        rel = float(c1.iloc[-1] / c1.iloc[-13] - 1) - btc["ret12"]
        if rel >= 0.015:
            vote("rs_btc", "L", "outperforming BTC over 12h")
        elif rel <= -0.015:
            vote("rs_btc", "S", "underperforming BTC over 12h")

    atr_exp = float(atr(d1h).iloc[-1] / atr(d1h).tail(50).mean())
    if adx_v >= 25:
        regime = "Trending"
    elif adx_v < 18:
        regime = "Ranging"
    else:
        regime = "Transitional"
    if atr_exp >= 1.5:
        regime += ", volatile"

    return {"votes": votes, "atr1": atr1, "regime": regime, "sup": sup, "res": res}


# --------------------------------------------------------------------------
# Gate, levels, confidence, leverage
# --------------------------------------------------------------------------
def build_levels(side, price, atr1, protect, target):
    """Entry zone, stop, TP1, TP2. protect/target = [(price, label)]."""
    sgn = 1 if side == "L" else -1
    zw = 0.3 * atr1
    lo, hi = (price - zw, price) if sgn == 1 else (price, price + zw)
    mid = (lo + hi) / 2
    far = lo if sgn == 1 else hi

    cands = [(sgn * (far - p), p, lab) for p, lab in protect if 0.7 * atr1 <= sgn * (far - p) <= 3 * atr1]
    if cands:
        _, lvl, stop_lab = min(cands)
        stop = lvl - sgn * 0.15 * atr1
    else:
        stop, stop_lab = far - sgn * 1.5 * atr1, "ATR-based"
    risk = sgn * (mid - stop)
    if risk <= 0:
        return None

    tcands = []
    for p, lab in target:
        p_adj = p - sgn * 0.05 * atr1
        d = sgn * (p_adj - mid)
        if risk <= d <= 3 * risk:
            tcands.append((d, p_adj, lab))
    if tcands:
        d1, tp1, tp1_lab = min(tcands)
    else:
        d1, tp1, tp1_lab = 1.5 * risk, mid + sgn * 1.5 * risk, "ATR-based"
    d2 = max(2.5 * risk, d1 + 0.6 * risk)
    tp2 = mid + sgn * d2

    # consistency pass
    assert sgn * (far - stop) > 0 and sgn * (tp1 - (hi if sgn == 1 else lo)) > 0 and sgn * (tp2 - tp1) > 0
    return {"lo": lo, "hi": hi, "stop": stop, "stop_lab": stop_lab, "tp1": tp1,
            "tp1_lab": tp1_lab, "tp2": tp2, "rr": d1 / risk, "tp1_dist": d1, "mid": mid}


def decide(coin, tier, price, ev, btc):
    """Return a signal dict, or (None, reason)."""
    votes = ev["votes"]
    ls = sum(v.w for v in votes if v.side == "L")
    ss = sum(v.w for v in votes if v.side == "S")
    side = "L" if ls >= ss else "S"
    win, opp = (ls, ss) if side == "L" else (ss, ls)
    wv = [v for v in votes if v.side == side]
    ov = [v for v in votes if v.side != side]
    pts = {n: sum(v.w for v in wv if v.layer == n) for n in range(1, 6)}
    layers = [n for n, p in pts.items() if p >= 0.5]
    tag = f"{coin} L={ls:.2f} S={ss:.2f} layers={len(layers)}"

    if win < MIN_SCORE:
        return None, f"{tag} score below {MIN_SCORE}"
    if len(layers) < MIN_LAYERS:
        return None, f"{tag} too few layers"
    n_mom = sum(1 for v in wv if v.layer == 3)
    trend_route = pts[3] >= 1.0 and n_mom >= 2
    names = {v.name for v in wv}
    rev_route = len(names & REVERSAL_CLUSTER) >= 3 and bool(names & {"rsi_div", "sr_prox"})
    if REQUIRE_ROUTE and not (trend_route or rev_route):
        return None, f"{tag} no trend/reversal confirmation"
    if opp > MAX_OPP_RATIO * win or win - opp < MIN_MARGIN:
        return None, f"{tag} too much disagreement (opp={opp:.2f})"
    shock = btc.get("shock", 0)
    if (shock == -1 and side == "L") or (shock == 1 and side == "S"):
        return None, f"{tag} against BTC shock"

    protect = ev["sup"] if side == "L" else ev["res"]
    target = ev["res"] if side == "L" else ev["sup"]
    lv = build_levels(side, price, ev["atr1"], protect, target)
    if lv is None or lv["rr"] < MIN_RR:
        return None, f"{tag} poor risk/reward"

    route = "Trend (momentum-confirmed)" if trend_route else "Reversal (early setup)"
    margin = win - opp
    conf = (40 + 2.5 * win + 2.5 * len(layers) + 1.5 * min(margin, 8) - 2.0 * opp
            - (8 if shock else 0) - (0 if trend_route else 4) - (3 if tier == 3 else 0)
            - (3 if ev["regime"].startswith("Ranging") else 0))
    conf = int(max(30, min(95, round(conf))))

    lev = 5 if conf >= 80 else 4 if conf >= 72 else 3 if conf >= 64 else 2
    if opp >= 2.5:
        lev = min(lev, 2)
    elif opp >= 1.5:
        lev = min(lev, 3)
    if shock:
        lev -= 1
    if not trend_route:
        lev -= 1
    if tier == 3:
        lev = min(lev, 3)
    lev = max(1, lev)

    strength = "A (Very strong)" if win >= 8.5 else "B (Strong)" if win >= 7.0 else "C (Moderate)"
    hours = int(max(2, min(24, round(lv["tp1_dist"] / ev["atr1"] * 1.5))))
    return {
        "coin": coin, "side": side, "price": price, "score": win, "opp": opp,
        "layers": len(layers), "layer_names": [LAYER_NAMES[n] for n in layers],
        "route": route, "regime": ev["regime"], "conf": conf, "lev": lev,
        "strength": strength, "hours": hours, "levels": lv,
        "for": sorted(wv, key=lambda v: -v.w), "against": sorted(ov, key=lambda v: -v.w),
    }, tag + " -> SIGNAL"


# --------------------------------------------------------------------------
# Telegram message
# --------------------------------------------------------------------------
def fp(x):
    ax = abs(x)
    dec = 2 if ax >= 100 else 3 if ax >= 10 else 4 if ax >= 1 else 5 if ax >= 0.1 else 6 if ax >= 0.01 else 8
    return f"{x:,.{dec}f}"


def format_signal(sig):
    e = html.escape
    lv, long_ = sig["levels"], sig["side"] == "L"
    sym = sig["coin"] + "USDT"
    why = "; ".join(v.text for v in sig["for"][:6])
    extra = len(sig["for"]) - 6
    if extra > 0:
        why += f" (+{extra} more)"
    lines = [
        f"{'🟢' if long_ else '🔴'} <b>NEW SIGNAL — {e(sym)}</b>",
        "",
        f"<b>Direction:</b> {'LONG' if long_ else 'SHORT'}",
        f"<b>Strength:</b> Tier {e(sig['strength'])} · score {sig['score']:.1f}/11",
        f"<b>Confidence:</b> {sig['conf']}%",
        f"<b>Regime:</b> {e(sig['regime'])}",
        f"<b>Gate route:</b> {e(sig['route'])}",
        f"<b>Layers:</b> {sig['layers']}/5 ({e(', '.join(sig['layer_names']))})",
        "",
        f"<b>Entry zone:</b> <code>{fp(lv['lo'])} – {fp(lv['hi'])}</code>",
        f"<b>Stop-loss:</b> <code>{fp(lv['stop'])}</code> ({e(lv['stop_lab'])})",
        f"<b>Take-profit 1:</b> <code>{fp(lv['tp1'])}</code> ({e(lv['tp1_lab'])})",
        f"<b>Take-profit 2:</b> <code>{fp(lv['tp2'])}</code>",
        f"<b>Risk/Reward (to TP1):</b> {lv['rr']:.1f}",
        f"<b>Suggested leverage:</b> {sig['lev']}x max",
        f"<b>Estimated validity:</b> ~{sig['hours']} hours",
        "",
        f"<b>Why this fired:</b> {e(why)}.",
    ]
    if sig["against"]:
        lines.append(f"<b>Against:</b> {e('; '.join(v.text for v in sig['against'][:3]))}.")
    lines.append("<b>Watch for:</b> " + (
        "a sharp BTC drop or loss of the stop level would invalidate this long."
        if long_ else
        "a sharp BTC rally or a reclaim of the stop level would invalidate this short."))
    lines += ["", "⚠️ Not financial advice. Trade at your own risk."]
    return "\n".join(lines)


def chunk_message(text, limit=4096):
    """Split on line breaks so each part fits Telegram's 4096-char limit
    (same idea as the notifier in cryptosignal/crypto-signal)."""
    if len(text) <= limit:
        return [text]
    parts, cur = [], ""
    for line in text.split("\n"):
        while len(line) > limit:
            line, rest = line[:limit], line[limit:]
            parts.append(line)
            line = rest
        if len(cur) + len(line) + 1 > limit:
            parts.append(cur)
            cur = line
        else:
            cur = f"{cur}\n{line}" if cur else line
    if cur:
        parts.append(cur)
    return parts


def tg_send(text):
    token, chat = os.getenv("TELEGRAM_BOT_TOKEN"), os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat:
        raise RuntimeError("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set")
    for part in chunk_message(text):
        for attempt in range(3):  # retry on timeout, like the original notifier
            try:
                r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                                  json={"chat_id": chat, "text": part, "parse_mode": "HTML",
                                        "disable_web_page_preview": True}, timeout=20)
                break
            except requests.Timeout:
                if attempt == 2:
                    raise RuntimeError("Telegram timeout") from None
                time.sleep(5)
            except Exception as e:  # never leak the token in logs
                raise RuntimeError(str(e).replace(token, "***")) from None
        if not r.ok:
            raise RuntimeError(f"Telegram error {r.status_code}: {r.text[:200].replace(token, '***')}")


# --------------------------------------------------------------------------
# State / dedup
# --------------------------------------------------------------------------
def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {"last": {}}


def save_state(state):
    cutoff = time.time() - 48 * 3600
    state["last"] = {k: v for k, v in state.get("last", {}).items() if v["ts"] >= cutoff}
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def is_duplicate(state, sig, now):
    key = f"{sig['coin']}|{sig['side']}"
    prev = state.get("last", {}).get(key)
    if not prev:
        return False
    age_h = (now - prev["ts"]) / 3600
    if age_h < COOLDOWN_H:
        return True
    if age_h >= HARD_EXPIRY_H:
        return False
    moved = abs(sig["price"] - prev["price"])
    return moved < 0.75 * prev["tp1_dist"]


# --------------------------------------------------------------------------
# Scan
# --------------------------------------------------------------------------
def btc_context(ex):
    try:
        d1h = ex.klines("BTC", "1h", 60)
        d15 = ex.klines("BTC", "15m", 30)
        closed = d1h.iloc[:-1]
        ret12 = float(closed.c.iloc[-1] / closed.c.iloc[-13] - 1)
        chg = float(d15.c.iloc[-1] / d15.c.iloc[-5] - 1)
        shock = (1 if chg > 0 else -1) if abs(chg) >= SHOCK_PCT else 0
        return {"ret12": ret12, "shock": shock, "chg1h": chg}
    except Exception as e:
        log.warning("BTC context unavailable: %s", e)
        return {"ret12": None, "shock": 0, "chg1h": 0.0}


def analyze_coin(ex, coin, tier, btc):
    try:
        d15 = ex.klines(coin, "15m", 120)
        d1h = ex.klines(coin, "1h", 220)
        d4h = ex.klines(coin, "4h", 260)
    except Exception as e:
        log.warning("%s: data fetch failed (%s)", coin, e)
        return None
    if len(d15) < 60 or len(d1h) < 120 or len(d4h) < 60:
        log.warning("%s: not enough history", coin)
        return None
    try:
        fund = ex.funding(coin)
    except Exception:
        fund = None
    try:
        oi = ex.oi(coin)
    except Exception:
        oi = None
    price = float(d15.c.iloc[-1])  # live price (forming candle)
    d15, d1h, d4h = (d.iloc[:-1].reset_index(drop=True) for d in (d15, d1h, d4h))  # closed only
    try:
        ev = evaluate(coin, price, d15, d1h, d4h, fund, oi, btc)
        if ev is None:
            return None
        sig, why = decide(coin, tier, price, ev, btc)
    except Exception as e:
        log.exception("%s: analysis error: %s", coin, e)
        return None
    log.info(why)
    return sig


def scan(dry=False, ex=None, btc=None):
    if ex is None:
        ex = pick_exchange(make_session())
    btc = btc or btc_context(ex)
    log.info("BTC: 12h %s, 1h change %.2f%%, shock=%s",
             btc.get("ret12"), btc.get("chg1h", 0) * 100, btc.get("shock"))
    state = load_state()
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        results = list(pool.map(lambda w: analyze_coin(ex, w[0], w[1], btc), WATCHLIST))
    sigs = [s for s in results if s]
    sigs.sort(key=lambda s: (-s["score"], -s["conf"]))
    now, sent = time.time(), 0
    for s in sigs:
        if is_duplicate(state, s, now):
            log.info("%s %s suppressed (cooldown / price not moved)", s["coin"], s["side"])
            continue
        if sent >= MAX_SIGNALS:
            break
        msg = format_signal(s)
        if dry:
            print(msg, "\n" + "-" * 40)
        else:
            tg_send(msg)
            state.setdefault("last", {})[f"{s['coin']}|{s['side']}"] = {
                "ts": now, "price": s["price"], "tp1_dist": s["levels"]["tp1_dist"]}
        sent += 1
    if not dry:
        save_state(state)
    log.info("scan done: %d candidates, %d sent", len(sigs), sent)
    return sent


# --------------------------------------------------------------------------
# Offline self-test (synthetic data, no network)
# --------------------------------------------------------------------------
def _synth(n, step_min, seed, drift=0.0, vol=0.004, start=100.0):
    rng = np.random.default_rng(seed)
    c = start * np.exp(np.cumsum(rng.normal(drift, vol, n)))
    o = np.concatenate([[start], c[:-1]])
    spread = np.abs(rng.normal(0, vol * 0.6, n)) * c
    return pd.DataFrame({"t": np.arange(n) * step_min * 60_000, "o": o,
                         "h": np.maximum(o, c) + spread, "l": np.minimum(o, c) - spread,
                         "v": rng.lognormal(10, 0.4, n), "c": c})


class FakeExchange:
    name = "fake"
    STEP = {"15m": 15, "1h": 60, "4h": 240}

    def klines(self, coin, interval, limit):
        seed = zlib.crc32(f"{coin}{interval}".encode())
        drift = ((seed % 7) - 3) * 0.0004
        return _synth(limit, self.STEP[interval], seed, drift=drift)

    def funding(self, coin):
        return [(-1) ** zlib.crc32(coin.encode()) * 0.0002] * 6

    def oi(self, coin):
        return list(np.linspace(1000, 1050, 6))


def selftest():
    global MIN_SCORE, MIN_LAYERS, MIN_MARGIN, MAX_OPP_RATIO, MIN_RR, REQUIRE_ROUTE, COOLDOWN_H
    ok = True
    # 1) strict defaults: random noise should (almost) never pass the gate
    ex = FakeExchange()
    btc = {"ret12": 0.0, "shock": 0, "chg1h": 0.0}
    strict = [analyze_coin(ex, c, t, btc) for c, t in WATCHLIST]
    print(f"strict gate on synthetic noise: {sum(1 for s in strict if s)}/{len(WATCHLIST)} signals")
    # 2) relaxed gate so the formatter and level builder get exercised
    MIN_SCORE, MIN_LAYERS, MIN_MARGIN, MAX_OPP_RATIO, MIN_RR, REQUIRE_ROUTE = 2.0, 2, 0.5, 1.0, 0.5, False
    relaxed = [s for s in (analyze_coin(ex, c, t, btc) for c, t in WATCHLIST) if s]
    print(f"relaxed gate: {len(relaxed)}/{len(WATCHLIST)} signals")
    if not relaxed:
        print("FAIL: no signal produced even with relaxed gate")
        return 1
    for s in relaxed:
        lv = s["levels"]
        long_ = s["side"] == "L"
        good = (lv["stop"] < lv["lo"] < lv["hi"] < lv["tp1"] < lv["tp2"]) if long_ else \
               (lv["stop"] > lv["hi"] > lv["lo"] > lv["tp1"] > lv["tp2"])
        if not good:
            ok = False
            print("FAIL level ordering:", s["coin"], s["side"], lv)
    print(format_signal(relaxed[0]))
    # 3) dedup logic
    st = {"last": {}}
    s0 = relaxed[0]
    now = time.time()
    st["last"][f"{s0['coin']}|{s0['side']}"] = {"ts": now - 3600, "price": s0["price"], "tp1_dist": s0["levels"]["tp1_dist"]}
    dup1 = is_duplicate(st, s0, now)
    st["last"][f"{s0['coin']}|{s0['side']}"]["ts"] = now - 5 * 3600
    dup2 = is_duplicate(st, s0, now)  # same price after cooldown -> still stale
    far = dict(s0, price=s0["price"] + 10 * s0["levels"]["tp1_dist"])
    dup3 = is_duplicate(st, far, now)
    print(f"dedup: within cooldown={dup1} (want True), same price later={dup2} (want True), moved far={dup3} (want False)")
    ok = ok and dup1 and dup2 and not dup3
    print("SELFTEST", "OK" if ok else "FAILED")
    return 0 if ok else 1


# --------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--scan", action="store_true", help="scan and send signals (default)")
    g.add_argument("--test", action="store_true", help="send a test message")
    g.add_argument("--dry-run", action="store_true", help="scan, print only")
    g.add_argument("--selftest", action="store_true", help="offline check")
    args = ap.parse_args()
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(message)s")
    if args.selftest:
        sys.exit(selftest())
    if args.test:
        tg_send("✅ Signal bot connected. Signals will appear here when a setup clears the gate.")
        print("test message sent")
        return
    scan(dry=args.dry_run)


if __name__ == "__main__":
    main()
