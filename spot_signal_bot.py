#!/usr/bin/env python3
"""
Binance SPOT signal bot -> Telegram.  Long-only (BUY) signals, 100% free data,
no API keys except your own Telegram bot token.

Layers (each indicator votes, weights add up to a score):
  1 Trend      4H EMA 10/20/55 stack, EMA cross, EMA pullback bounce, 1D SMA50/SMA100
  2 Levels     Fibonacci retracement (golden zone), bullish Fair Value Gap retest,
               rising trendline support bounce
  3 Patterns   inverse Head&Shoulders, double bottom, cup & handle, triangles /
               falling wedge, falling-trendline breakout, bull flag
               (bearish H&S / double top = veto)
  4 Volume     volume surge, OBV, Binance taker-buy ratio (real order flow)
  5 Momentum   MACD, RSI divergence, RSI zone
  6 Context    BTC trend + shock filter, Fear & Greed index (sentiment),
               stablecoin supply trend + chain TVL (DefiLlama = free on-chain)

Gate: score, >=3 of layers 1-5, trend/reversal route, volume confirmation for
breakouts, not over-extended, no room-blocking resistance, R/R, stop distance.
A signal tracker follows every sent signal (TP1/TP2/TP3/SL) so you get a real
forward track record.

All weights/thresholds are my own choices and are NOT backtested.

  python spot_signal_bot.py --test       send a test message
  python spot_signal_bot.py --scan       scan + send signals (default)
  python spot_signal_bot.py --dry-run    scan, print only, save nothing
  python spot_signal_bot.py --stats      print forward track record
  python spot_signal_bot.py --square-test  post one test line to Binance Square (needs key)
  python spot_signal_bot.py --selftest   offline checks on synthetic data
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

log = logging.getLogger("spot")


# --------------------------------------------------------------------------
# Config (override with environment variables)
# --------------------------------------------------------------------------
def _num(name, default):
    v = os.getenv(name)
    return float(v) if v not in (None, "") else float(default)


MIN_SCORE = _num("MIN_SCORE", 6.0)
MIN_LAYERS = int(_num("MIN_LAYERS", 3))       # of layers 1-5
MIN_RR = _num("MIN_RR", 1.5)                  # TP1 must be >= this many R away
MAX_RISK_PCT = _num("MAX_RISK_PCT", 8.0)      # skip if stop is farther than this %
TOP_N = int(_num("TOP_N", 0))                 # 0 = scan EVERY USDT spot pair
MIN_QUOTE_VOL = _num("MIN_QUOTE_VOL", 1_000_000)   # 24h USDT volume floor (0 = no floor)
SCAN_BUDGET_S = _num("SCAN_BUDGET_S", 540)    # stop analysing after this many seconds
SQUARE_GAP_S = _num("SQUARE_GAP_S", 12)      # seconds between Binance Square posts
COOLDOWN_H = _num("COOLDOWN_H", 12)
HARD_EXPIRY_H = _num("HARD_EXPIRY_H", 48)
MAX_SIGNALS = int(_num("MAX_SIGNALS", 3))
TRACK_DAYS = _num("TRACK_DAYS", 7)
WORKERS = int(_num("WORKERS", 6))
MA_FAST = int(_num("MA_FAST", 50))            # daily SMAs ("MA 50/100")
MA_SLOW = int(_num("MA_SLOW", 100))
STATE_FILE = os.getenv("STATE_FILE", "spot_state.json")
REQUIRE_GATE = True                           # selftest relaxes this

BASES = ([os.getenv("BINANCE_BASE")] if os.getenv("BINANCE_BASE") else
         ["https://data-api.binance.vision", "https://api.binance.com",
          "https://api1.binance.com", "https://api2.binance.com", "https://api3.binance.com"])

STABLES = {"USDC", "FDUSD", "TUSD", "USDP", "DAI", "BUSD", "USDE", "USD1", "EUR", "AEUR",
           "XUSD", "PYUSD", "RLUSD", "EURI", "GBP", "TRY", "BRL", "UST", "USDS", "BFUSD"}
# Tokenized stocks / ETFs (e.g. TSLAX, NVDAON) are not crypto -> skipped.
STOCK_TICKERS = {"AAPL", "TSLA", "NVDA", "MSFT", "GOOGL", "GOOG", "AMZN", "META", "NFLX", "AMD",
                 "INTC", "MSTR", "COIN", "HOOD", "CRCL", "PLTR", "BABA", "DIS", "PYPL", "UBER",
                 "SHOP", "ORCL", "AVGO", "JPM", "WMT", "PEP", "NKE", "SPY", "QQQ", "IWM", "GLD",
                 "SLV", "TLT", "SNAP", "ABNB", "XOM", "CVX", "GME", "AMC", "MCD", "ADBE", "CRM",
                 "TSM", "ASML", "ARM", "SMCI", "QCOM", "NIO", "BRKB", "LLY", "UNH", "COST"}
STOCK_EXACT = {"AAPL", "TSLA", "NVDA", "MSFT", "GOOGL", "AMZN", "MSTR", "NFLX", "PLTR", "SPY", "QQQ"}
EXTRA_EXCLUDE = {x.strip().upper() for x in os.getenv("EXCLUDE", "").split(",") if x.strip()}


def is_stock_token(base):
    if base in EXTRA_EXCLUDE or base in STOCK_EXACT:
        return True
    return any(base.endswith(suf) and base[:-len(suf)] in STOCK_TICKERS for suf in ("ON", "X"))


FALLBACK = ["BTC", "ETH", "BNB", "SOL", "XRP", "DOGE", "ADA", "AVAX", "LINK", "DOT", "LTC",
            "BCH", "NEAR", "TRX", "UNI", "AAVE", "ATOM", "APT", "ARB", "OP", "SUI", "INJ",
            "TON", "HBAR", "FIL", "ETC", "TIA", "SEI", "PEPE", "WLD", "ENA", "RUNE"]
CHAIN_MAP = {"ETH": "Ethereum", "SOL": "Solana", "BNB": "BSC", "AVAX": "Avalanche",
             "ARB": "Arbitrum", "OP": "Optimism", "SUI": "Sui", "APT": "Aptos", "NEAR": "Near",
             "TRX": "Tron", "TON": "TON", "POL": "Polygon", "MATIC": "Polygon",
             "INJ": "Injective", "SEI": "Sei", "S": "Sonic", "ADA": "Cardano",
             "ALGO": "Algorand", "HBAR": "Hedera", "FTM": "Fantom"}

# Nominal weights (max per indicator) and layers. 1 Trend 2 Levels 3 Patterns
# 4 Volume 5 Momentum 6 Context
W = {
    "ema_stack": 1.0, "ema_cross": 0.6, "ema_support": 0.5, "ma50_100": 0.75,
    "fib": 1.0, "fvg": 0.9, "tl_support": 0.5,
    "ihs": 1.25, "dbl_bottom": 0.9, "cup_handle": 1.25, "triangle": 1.0,
    "trend_break": 1.0, "bull_flag": 0.9,
    "vol_surge": 1.0, "obv": 0.5, "taker": 0.5,
    "macd4h": 0.5, "rsi_div": 0.7, "rsi_zone": 0.4,
    "btc_trend": 0.5, "sentiment": 0.4, "stable_flow": 0.4, "chain_tvl": 0.4,
}
LAYER = {}
for _l, _names in {1: ("ema_stack", "ema_cross", "ema_support", "ma50_100"),
                   2: ("fib", "fvg", "tl_support"),
                   3: ("ihs", "dbl_bottom", "cup_handle", "triangle", "trend_break", "bull_flag"),
                   4: ("vol_surge", "obv", "taker"),
                   5: ("macd4h", "rsi_div", "rsi_zone"),
                   6: ("btc_trend", "sentiment", "stable_flow", "chain_tvl")}.items():
    for _n in _names:
        LAYER[_n] = _l
LAYER_NAMES = {1: "Trend", 2: "Levels", 3: "Pattern", 4: "Volume", 5: "Momentum", 6: "Context"}
LCAP = {3: 1.5, 6: 1.5}  # one layer must not dominate the score
MAX_SCORE = sum(min(LCAP.get(l, 99), sum(W[n] for n in W if LAYER[n] == l)) for l in range(1, 7))

# short, human wording of each vote for Binance Square posts
SHORT = {
    "ema_stack": "EMA 10/20/55 trend stack", "ema_cross": "fresh EMA cross",
    "ema_support": "bounce from EMA support", "ma50_100": "price above daily MA50/MA100",
    "fib": "Fibonacci golden-zone pullback", "fvg": "fair value gap holding as support",
    "tl_support": "rising trendline support",
    "ihs": "inverse head & shoulders breakout", "dbl_bottom": "double-bottom breakout",
    "cup_handle": "cup & handle breakout", "triangle": "triangle/wedge breakout",
    "trend_break": "falling trendline breakout", "bull_flag": "bull flag breakout",
    "vol_surge": "volume surge", "obv": "steady accumulation (OBV)",
    "taker": "aggressive buyers (taker flow)", "macd4h": "MACD turning up",
    "rsi_div": "bullish RSI divergence", "rsi_zone": "healthy RSI momentum",
    "btc_trend": "supportive BTC trend", "sentiment": "fear-driven dip",
    "stable_flow": "fresh stablecoin liquidity", "chain_tvl": "growing on-chain activity",
}

Vote = namedtuple("Vote", "name layer w text")


# --------------------------------------------------------------------------
# Indicator math (numpy / pandas only)
# --------------------------------------------------------------------------
def ema(s, n):
    return s.ewm(span=n, adjust=False).mean()


def sma(s, n):
    return s.rolling(n).mean()


def rsi(s, n=14):
    d = s.diff()
    up = d.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean()
    out = 100 - 100 / (1 + up / dn.replace(0, np.nan))
    return out.where(dn != 0, pd.Series(np.where(up > 0, 100.0, 50.0), index=s.index)).fillna(50.0)


def macd_hist(s):
    m = ema(s, 12) - ema(s, 26)
    return m - ema(m, 9)


def atr(df, n=14):
    pc = df.c.shift(1)
    tr = pd.concat([df.h - df.l, (df.h - pc).abs(), (df.l - pc).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / n, adjust=False).mean()


def obv(df):
    return (np.sign(df.c.diff().fillna(0)) * df.v).cumsum()


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


def crossed_up(a, b, lookback):
    s = (a > b).values
    return bool(s[-1] and (~s[-(lookback + 1):-1]).any())


def fp(x):
    ax = abs(x)
    dec = 2 if ax >= 100 else 3 if ax >= 10 else 4 if ax >= 1 else 5 if ax >= 0.1 else 6 if ax >= 0.01 else 8
    return f"{x:,.{dec}f}"


# --------------------------------------------------------------------------
# Chart pattern detectors (4H, long-side). Each returns a dict or None:
#   name, level (breakout level), target (measured move, may be None), stop
# --------------------------------------------------------------------------
def pat_inverse_hs(df, atr_v):
    hi, lo = swings(df, 3)
    n = len(df)
    c, h, l = df.c.values, df.h.values, df.l.values
    for j in range(len(lo) - 1, 1, -1):
        (i1, p1), (i2, p2), (i3, p3) = lo[j - 2], lo[j - 1], lo[j]
        if n - 1 - i3 > 35:
            break
        if not (p2 < p1 * 0.98 and p2 < p3 * 0.98):
            continue
        if abs(p1 - p3) / max(p1, p3) > 0.07 or not (10 <= i3 - i1 <= 120):
            continue
        if l[i1:i3 + 1].min() < p2 * 0.999:
            continue
        pk1, pk2 = h[i1:i2 + 1].max(), h[i2:i3 + 1].max()
        if abs(pk1 - pk2) / max(pk1, pk2) > 0.06:
            continue
        neck = (pk1 + pk2) / 2
        if c[-1] > neck * 1.001 and (c[-9:-1] <= neck).any() and c[-1] - neck <= 2.5 * atr_v:
            return {"name": "Inverse Head & Shoulders", "level": neck,
                    "target": neck + (neck - p2), "stop": p3 - 0.2 * atr_v}
    return None


def pat_double_bottom(df, atr_v):
    hi, lo = swings(df, 3)
    n = len(df)
    c, h = df.c.values, df.h.values
    for j in range(len(lo) - 1, 0, -1):
        (i1, p1), (i2, p2) = lo[j - 1], lo[j]
        if n - 1 - i2 > 40:
            break
        if not (8 <= i2 - i1 <= 90) or abs(p1 - p2) / max(p1, p2) > 0.03:
            continue
        pk = h[i1:i2 + 1].max()
        bot = min(p1, p2)
        if (pk - bot) / bot < 0.05:
            continue
        if c[-1] > pk * 1.001 and (c[-9:-1] <= pk).any() and c[-1] - pk <= 2.5 * atr_v:
            return {"name": "Double bottom", "level": pk, "target": pk + (pk - bot),
                    "stop": bot - 0.2 * atr_v}
    return None


def pat_cup_handle(df, atr_v):
    hi, _ = swings(df, 3)
    n = len(df)
    c, l = df.c.values, df.l.values
    best = None
    for ja in range(len(hi) - 1):
        for jc in range(ja + 1, len(hi)):
            (ia, pa), (ic, pc) = hi[ja], hi[jc]
            span = ic - ia
            if not 25 <= span <= 130 or abs(pa - pc) / max(pa, pc) > 0.05:
                continue
            if not 2 <= n - 1 - ic <= 25:
                continue
            rim = max(pa, pc)
            b = int(l[ia:ic + 1].argmin()) + ia
            pb = l[b]
            depth = (rim - pb) / rim
            if not 0.08 <= depth <= 0.45 or not 0.25 <= (b - ia) / span <= 0.75:
                continue
            x = np.arange(ia, ic + 1)
            y = c[ia:ic + 1]
            co = np.polyfit(x, y, 2)
            fit = np.polyval(co, x)
            ss_tot = ((y - y.mean()) ** 2).sum()
            r2 = 1 - ((y - fit) ** 2).sum() / ss_tot if ss_tot > 0 else 0
            u_frac = float((y <= pb + 0.33 * (rim - pb)).mean())  # U-shape, not V
            if co[0] <= 0 or r2 < 0.6 or u_frac < 0.35:
                continue
            handle_low = l[ic:n].min()
            if handle_low < rim - 0.5 * (rim - pb) or handle_low < pb:
                continue
            if c[-1] > rim * 1.001 and (c[-7:-1] <= rim).any() and c[-1] - rim <= 2.5 * atr_v:
                best = {"name": "Cup & Handle", "level": rim, "target": rim + (rim - pb),
                        "stop": handle_low - 0.2 * atr_v}
    return best


def _fit(pts):
    x = np.array([i for i, _ in pts], float)
    y = np.array([p for _, p in pts], float)
    slope, inter = np.polyfit(x, y, 1)
    return float(slope), float(inter)


def pat_triangle(df, atr_v, win=70):
    n = len(df)
    c = df.c.values
    hi, lo = swings(df, 3)
    hi = [(i, p) for i, p in hi if i >= n - win][-4:]
    lo = [(i, p) for i, p in lo if i >= n - win][-4:]
    if len(hi) < 2 or len(lo) < 2:
        return None
    sh, ih = _fit(hi)
    sl, il = _fit(lo)
    i0 = min(hi[0][0], lo[0][0])
    span = max(hi[-1][0], lo[-1][0]) - i0
    if span < 15:
        return None
    pr = c[-1]
    fh, fl = sh * span / pr, sl * span / pr
    if abs(fh) <= 0.015 and fl >= 0.02:
        kind = "Ascending triangle"
    elif fh <= -0.02 and fl >= 0.02:
        kind = "Symmetrical triangle"
    elif fh <= -0.03 and -0.05 <= fl <= -0.005 and fl > fh:
        kind = "Falling wedge"
    else:
        return None
    res_now = sh * (n - 1) + ih
    sup_now = sl * (n - 1) + il
    if sup_now >= pr or c[-1] <= res_now * 1.002 or c[-1] - res_now > 2.5 * atr_v:
        return None
    idx = np.arange(n - 6, n - 1)
    if not (c[n - 6:n - 1] <= (sh * idx + ih) * 1.001).any():
        return None
    height = (sh * i0 + ih) - (sl * i0 + il)
    return {"name": kind, "level": res_now, "target": res_now + max(height, 0),
            "stop": min(lo[-1][1], sup_now) - 0.2 * atr_v}


def pat_trend_break(df, atr_v):
    n = len(df)
    c = df.c.values
    hi, lo = swings(df, 3)
    hi = [(i, p) for i, p in hi if i >= n - 90]
    if len(hi) < 2:
        return None
    (i1, p1), (i2, p2) = hi[-2], hi[-1]
    if not (p2 < p1 * 0.99 and i2 - i1 >= 6 and n - 1 - i2 <= 45):
        return None
    slope = (p2 - p1) / (i2 - i1)
    line = lambda i: p2 + slope * (i - i2)
    if not (c[-1] > line(n - 1) * 1.003 and c[-1] - line(n - 1) <= 2.5 * atr_v):
        return None
    if not any(c[i] <= line(i) for i in range(n - 6, n - 1)):
        return None
    lows_after = [p for i, p in lo if i > i2]
    stop = (min(lows_after) if lows_after else df.l.values[i2:].min()) - 0.2 * atr_v
    return {"name": "Falling trendline breakout", "level": line(n - 1), "target": p1, "stop": stop}


def pat_bull_flag(df, atr_v):
    n = len(df)
    h, l, c = df.h.values, df.l.values, df.c.values
    lo_i = n - 30
    p = int(h[lo_i:n - 3].argmax()) + lo_i
    flag_n = n - 1 - p
    if not 3 <= flag_n <= 16:
        return None
    base = l[max(0, p - 14):p + 1].min()
    if (h[p] - base) / base < 0.08:
        return None
    pole = h[p] - base
    flag_low = l[p + 1:n].min()
    if flag_low < h[p] - 0.4 * pole:
        return None
    res = h[p + 1:n - 1].max() if n - 1 > p + 1 else h[p]
    if (res - l[p + 1:n - 1].min()) / pole > 0.45 if n - 1 > p + 1 else False:
        return None
    if c[-1] > res * 1.001 and c[-1] - res <= 2.0 * atr_v:
        return {"name": "Bull flag", "level": res, "target": res + 0.8 * pole,
                "stop": flag_low - 0.2 * atr_v}
    return None


def bearish_patterns(df, atr_v):
    """Confirmed bearish breakdowns (used as veto for new BUY signals)."""
    hi, _ = swings(df, 3)
    n = len(df)
    c, l = df.c.values, df.l.values
    out = []
    for j in range(len(hi) - 1, 1, -1):
        (i1, p1), (i2, p2), (i3, p3) = hi[j - 2], hi[j - 1], hi[j]
        if n - 1 - i3 > 40:
            break
        if p2 > p1 * 1.02 and p2 > p3 * 1.02 and abs(p1 - p3) / max(p1, p3) <= 0.07:
            neck = (l[i1:i2 + 1].min() + l[i2:i3 + 1].min()) / 2
            if c[-1] < neck * 0.999 and (c[-13:-1] >= neck).any():
                out.append("Head & Shoulders top breakdown")
                break
    for j in range(len(hi) - 1, 0, -1):
        (i1, p1), (i2, p2) = hi[j - 1], hi[j]
        if n - 1 - i2 > 40:
            break
        if 8 <= i2 - i1 <= 90 and abs(p1 - p2) / max(p1, p2) <= 0.03:
            valley = l[i1:i2 + 1].min()
            top = max(p1, p2)
            if (top - valley) / top >= 0.05 and c[-1] < valley * 0.999 and (c[-13:-1] >= valley).any():
                out.append("Double top breakdown")
                break
    return out


PATTERNS = [("ihs", pat_inverse_hs), ("dbl_bottom", pat_double_bottom),
            ("cup_handle", pat_cup_handle), ("triangle", pat_triangle),
            ("trend_break", pat_trend_break), ("bull_flag", pat_bull_flag)]


# --------------------------------------------------------------------------
# Fibonacci / Fair Value Gap / trendline support
# --------------------------------------------------------------------------
def fib_setup(d4, d1, price, atr4):
    n = len(d4)
    hi, lo = swings(d4, 3)
    h, l = d4.h.values, d4.l.values
    for ih, H in reversed(hi):
        if n - 1 - ih > 60:
            break
        lows = [(i, p) for i, p in lo if ih - 70 <= i < ih]
        if not lows:
            continue
        il, L = min(lows, key=lambda x: x[1])
        leg = H - L
        if leg / L < 0.08 or leg < 3 * atr4:
            continue
        if h[ih + 1:].size and h[ih + 1:].max() > H * 1.001:
            continue
        if l[il + 1:].min() < L * 0.999:
            continue
        r = (H - price) / leg
        if 0.382 <= r <= 0.70 and price > L and d1.c.iloc[-1] > d1.o.iloc[-1]:
            return {"L": L, "H": H, "r": r, "l786": H - 0.786 * leg,
                    "ext1272": L + 1.272 * leg, "ext1618": L + 1.618 * leg}
        return None
    return None


def bull_fvg(df, atr_v, lookback):
    n = len(df)
    h, l, o, c = df.h.values, df.l.values, df.o.values, df.c.values
    for i in range(n - 2, max(n - lookback, 3) - 1, -1):
        bottom, top = h[i - 2], l[i]
        if top <= bottom or top - bottom < 0.3 * atr_v:
            continue
        if not (c[i - 1] > o[i - 1] and c[i - 1] - o[i - 1] >= 0.7 * atr_v):
            continue
        if not (c[i + 1:] >= bottom).all():  # gap already filled/broken
            continue
        if l[-1] <= top * 1.003 and c[-1] >= bottom and c[-1] > o[-1]:
            return {"bottom": bottom, "top": top, "idx": i}
    return None


def tl_support(df, atr_v):
    n = len(df)
    _, lo = swings(df, 3)
    lo = [(i, p) for i, p in lo if i >= n - 100]
    if len(lo) < 2:
        return None
    (i1, p1), (i2, p2) = lo[-2], lo[-1]
    if not (i2 - i1 >= 6 and p2 > p1 * 1.005):
        return None
    slope = (p2 - p1) / (i2 - i1)
    line = p2 + slope * (n - 1 - i2)
    if n - 1 - i2 < 2:
        return None
    if df.l.iloc[-1] <= line + 0.3 * atr_v and df.c.iloc[-1] > line and df.c.iloc[-1] > df.o.iloc[-1]:
        return {"line": line, "stop": line - 0.25 * atr_v}
    return None


def rsi_bull_div(d4):
    r = rsi(d4.c).values
    _, lo = swings(d4.tail(70).reset_index(drop=True), 2)
    off = len(d4) - 70 if len(d4) >= 70 else 0
    if len(lo) < 2:
        return False
    (i1, p1), (i2, p2) = lo[-2], lo[-1]
    return bool(p2 < p1 and r[i2 + off] > r[i1 + off] and i2 - i1 >= 4 and len(d4) - 1 - (i2 + off) <= 8)


# --------------------------------------------------------------------------
# Data (public Binance spot endpoints, no key)
# --------------------------------------------------------------------------
def make_session():
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=0.7, status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=("GET",))
    s.mount("https://", HTTPAdapter(max_retries=retry, pool_maxsize=16))
    s.headers["User-Agent"] = "spot-signal-bot/1.0"
    return s


class Spot:
    name = "binance-spot"

    def __init__(self, session):
        self.s = session
        self.base = None

    def _get(self, path, **params):
        last = None
        for b in ([self.base] if self.base else BASES):
            try:
                r = self.s.get(b + path, params=params, timeout=15)
                if r.status_code in (403, 451):
                    last = RuntimeError(f"{b} blocked ({r.status_code})")
                    continue
                r.raise_for_status()
                self.base = b
                return r.json()
            except requests.RequestException as e:
                last = e
        raise RuntimeError(f"Binance unreachable: {last}")

    def klines(self, sym, interval, limit):
        d = self._get("/api/v3/klines", symbol=sym, interval=interval, limit=limit)
        df = pd.DataFrame(d).iloc[:, [0, 1, 2, 3, 4, 5, 9]]
        df.columns = ["t", "o", "h", "l", "c", "v", "tb"]
        return df.astype(float)

    def universe(self):
        rows = []
        for x in self._get("/api/v3/ticker/24hr"):
            s = x["symbol"]
            if not s.endswith("USDT"):
                continue
            base = s[:-4]
            if base in STABLES or is_stock_token(base):
                continue
            qv, last, chg = float(x["quoteVolume"]), float(x["lastPrice"]), float(x["priceChangePercent"])
            if qv < MIN_QUOTE_VOL or (0.97 <= last <= 1.03 and abs(chg) < 1.5):
                continue
            rows.append((qv, base))
        rows.sort(reverse=True)
        return [b for _, b in (rows[:TOP_N] if TOP_N > 0 else rows)]


def http_json(session, url, **params):
    r = session.get(url, params=params, timeout=15)
    r.raise_for_status()
    return r.json()


def get_context(session, ex, coins):
    """Market-wide context: sentiment, free on-chain data, BTC trend/shock."""
    ctx = {"fg": None, "stable7": None, "btc_up": None, "btc_shock": 0, "btc_chg4h": 0.0, "chain": {}}
    try:
        ctx["fg"] = int(http_json(session, "https://api.alternative.me/fng/", limit=1)["data"][0]["value"])
    except Exception as e:
        log.warning("Fear&Greed unavailable: %s", e)
    try:
        d = http_json(session, "https://stablecoins.llama.fi/stablecoincharts/all")
        vals = [float(x["totalCirculatingUSD"]["peggedUSD"]) for x in d[-9:]]
        ctx["stable7"] = vals[-1] / vals[-8] - 1
    except Exception as e:
        log.warning("Stablecoin supply unavailable: %s", e)
    for chain in sorted({CHAIN_MAP[c] for c in coins if c in CHAIN_MAP}):
        try:
            d = http_json(session, f"https://api.llama.fi/v2/historicalChainTvl/{chain}")
            ctx["chain"][chain] = float(d[-1]["tvl"]) / float(d[-8]["tvl"]) - 1
        except Exception as e:
            log.debug("TVL %s unavailable: %s", chain, e)
    try:
        b4 = ex.klines("BTCUSDT", "4h", 120)
        ctx["btc_up"] = bool(b4.c.iloc[-2] > ema(b4.c.iloc[:-1], 55).iloc[-1])
        b1 = ex.klines("BTCUSDT", "1h", 12)
        chg = float(b1.c.iloc[-1] / b1.c.iloc[-5] - 1)
        ctx["btc_chg4h"] = chg
        ctx["btc_shock"] = -1 if chg <= -0.025 else 0
    except Exception as e:
        log.warning("BTC context unavailable: %s", e)
    return ctx


# --------------------------------------------------------------------------
# Evaluation: every indicator casts a (weighted) BUY vote
# --------------------------------------------------------------------------
def evaluate(base, price, d1, d4, dd, ctx):
    V, stops, targets, notes = [], [], [], []

    def vote(name, w, text):
        V.append(Vote(name, LAYER[name], w, text))

    c1, c4 = d1.c, d4.c
    atr1, atr4 = float(atr(d1).iloc[-1]), float(atr(d4).iloc[-1])
    if not (atr1 > 0 and atr4 > 0):
        return None
    e10, e20, e55 = (float(ema(c4, n).iloc[-1]) for n in (10, 20, 55))

    # ---- 1. Trend ----
    if e10 > e20 > e55 and price > e20:
        vote("ema_stack", 1.0, "4H EMA 10>20>55 stack, price above EMA20")
    elif price > e55 and e20 > e55:
        vote("ema_stack", 0.5, "4H price above EMA55 and EMA20>EMA55")
    e10_1, e20_1, e55_1 = ema(c1, 10), ema(c1, 20), ema(c1, 55)
    if crossed_up(e10_1, e20_1, 6) and price > float(e55_1.iloc[-1]):
        vote("ema_cross", 0.6, "1H EMA10 just crossed above EMA20")
    elif crossed_up(ema(c4, 10), ema(c4, 20), 4) and price > e55:
        vote("ema_cross", 0.6, "4H EMA10 just crossed above EMA20")
    for k in (1, 2):
        lo_, cl_ = float(d4.l.iloc[-k]), float(d4.c.iloc[-k])
        if e20 > e55 and lo_ <= e20 * 1.004 and cl_ >= e20:
            vote("ema_support", 0.5, "bounce from 4H EMA20 in an uptrend")
            break
        if lo_ <= e55 * 1.004 and cl_ >= e55:
            vote("ema_support", 0.5, "bounce from 4H EMA55 support")
            break
    if len(dd) >= MA_SLOW + 2:
        sf, ss = float(sma(dd.c, MA_FAST).iloc[-1]), float(sma(dd.c, MA_SLOW).iloc[-1])
        if price > sf > ss:
            vote("ma50_100", 0.75, f"1D price > SMA{MA_FAST} > SMA{MA_SLOW}")
        elif price > sf:
            vote("ma50_100", 0.35, f"1D price above SMA{MA_FAST}")

    # ---- 2. Levels ----
    fib = fib_setup(d4, d1, price, atr4)
    if fib:
        golden = 0.5 <= fib["r"] <= 0.66
        vote("fib", 1.0 if golden else 0.6,
             f"pullback to Fibonacci {'golden zone ' if golden else ''}({fib['r']:.3f} retrace, "
             f"swing {fp(fib['L'])}->{fp(fib['H'])}) with bullish 1H candle")
        stops.append((fib["l786"] - 0.2 * atr4, "Fib 0.786"))
        targets += [(fib["H"], "swing high"), (fib["ext1272"], "Fib 1.272"), (fib["ext1618"], "Fib 1.618")]
    fvg4 = bull_fvg(d4, atr4, 30)
    fvg1 = bull_fvg(d1, atr1, 48)
    if fvg4 or fvg1:
        z, tf = (fvg4, "4H") if fvg4 else (fvg1, "1H")
        vote("fvg", 0.9 if fvg4 else 0.6,
             f"{tf} bullish Fair Value Gap {fp(z['bottom'])}-{fp(z['top'])} retested and held")
        stops.append((z["bottom"] - 0.15 * (atr4 if fvg4 else atr1), f"{tf} FVG low"))
    tl = tl_support(d4, atr4)
    if tl:
        vote("tl_support", 0.5, "bounce from rising 4H trendline support")
        stops.append((tl["stop"], "trendline"))

    # ---- 3. Chart patterns ----
    for key, fn in PATTERNS:
        try:
            p = fn(d4, atr4)
        except Exception as e:  # a detector bug must never kill the scan
            log.debug("%s %s failed: %s", base, key, e)
            p = None
        if p:
            vote(key, W[key], f"{p['name']} breakout above {fp(p['level'])} (4H)")
            stops.append((p["stop"], p["name"]))
            if p.get("target"):
                targets.append((p["target"], p["name"] + " target"))
    veto = bearish_patterns(d4, atr4)

    # ---- 4. Volume ----
    v = d1.v
    avg_v = float(v.iloc[-21:-1].mean()) if len(v) > 22 else 0
    surge = 0.0
    for k in (1, 2):
        if avg_v > 0 and d1.c.iloc[-k] > d1.o.iloc[-k]:
            rng = d1.h.iloc[-k] - d1.l.iloc[-k]
            strong = rng > 0 and (d1.c.iloc[-k] - d1.l.iloc[-k]) / rng >= 0.5
            ratio = float(v.iloc[-k] / avg_v)
            if strong and ratio >= 1.8:
                surge = max(surge, 1.0)
            elif strong and ratio >= 1.4:
                surge = max(surge, 0.5)
    v4 = d4.v
    if len(v4) > 22 and d4.c.iloc[-1] > d4.o.iloc[-1] and v4.iloc[-1] >= 1.6 * v4.iloc[-21:-1].mean():
        surge = max(surge, 0.8)
    if surge:
        vote("vol_surge", surge, "volume surge on a green candle (1H/4H)")
    ob = obv(d1)
    if ob.iloc[-1] > ema(ob, 20).iloc[-1] and ob.iloc[-1] > ob.iloc[-6]:
        vote("obv", 0.5, "1H on-balance volume rising (accumulation)")
    tv = float(d1.v.iloc[-3:].sum())
    if tv > 0 and float(d1.tb.iloc[-3:].sum()) / tv >= 0.54:
        vote("taker", 0.5, f"Binance taker-buy ratio {float(d1.tb.iloc[-3:].sum()) / tv:.0%} (buyers aggressive)")

    # ---- 5. Momentum ----
    h4 = macd_hist(c4)
    if h4.iloc[-1] > 0 and h4.iloc[-1] > h4.iloc[-2] or (h4.iloc[-1] > h4.iloc[-2] and (h4.iloc[-4:-1] < 0).all() and h4.iloc[-1] > -abs(h4.iloc[-4:-1]).min()):
        vote("macd4h", 0.5, "4H MACD momentum turning/rising up")
    if rsi_bull_div(d4):
        vote("rsi_div", 0.7, "bullish RSI divergence on 4H")
    r1 = rsi(c1)
    if 45 <= r1.iloc[-1] <= 68 and r1.iloc[-1] > r1.iloc[-2]:
        vote("rsi_zone", 0.4, f"1H RSI {r1.iloc[-1]:.0f} rising in healthy zone")

    # ---- 6. Context ----
    if base != "BTC" and ctx.get("btc_up") is not None:
        if ctx["btc_up"]:
            vote("btc_trend", 0.5, "BTC 4H trend is up (above EMA55)")
        else:
            vote("btc_trend", -0.4, "BTC is below its 4H EMA55 (weak market)")
    fg = ctx.get("fg")
    if fg is not None:
        if fg <= 25:
            vote("sentiment", 0.4, f"Fear & Greed {fg} (extreme fear, contrarian)")
        elif fg >= 80:
            vote("sentiment", -0.75, f"Fear & Greed {fg} (extreme greed, risk of top)")
        elif fg >= 70:
            vote("sentiment", -0.3, f"Fear & Greed {fg} (greed)")
    s7 = ctx.get("stable7")
    if s7 is not None:
        if s7 >= 0.003:
            vote("stable_flow", 0.4, f"stablecoin supply +{s7:.2%} in 7d (fresh liquidity)")
        elif s7 <= -0.003:
            vote("stable_flow", -0.4, f"stablecoin supply {s7:.2%} in 7d (liquidity leaving)")
    tvl = ctx.get("chain", {}).get(CHAIN_MAP.get(base))
    if tvl is not None:
        if tvl >= 0.03:
            vote("chain_tvl", 0.4, f"{CHAIN_MAP[base]} on-chain TVL +{tvl:.1%} in 7d")
        elif tvl <= -0.05:
            vote("chain_tvl", -0.4, f"{CHAIN_MAP[base]} on-chain TVL {tvl:.1%} in 7d")

    # ---- levels for stop / targets ----
    h4_, l4_ = swings(d4.tail(150).reset_index(drop=True), 3)
    resist = [p for _, p in h4_ if p > price]
    targets += [(p, "4H resistance") for p in resist]
    l1_ = swings(d1.tail(120).reset_index(drop=True), 3)[1]
    stops += [(p - 0.2 * atr4, "4H swing low") for _, p in l4_ if p < price]
    stops += [(p - 0.15 * atr1, "1H swing low") for _, p in l1_ if p < price]

    over = f"1H RSI {r1.iloc[-1]:.0f} overbought" if float(r1.iloc[-1]) > 78 else None
    over_ema = "price extended far above 4H EMA20" if price > e20 + 4 * atr4 else None
    return {"votes": V, "atr1": atr1, "atr4": atr4, "stops": stops, "targets": targets,
            "resist": sorted(resist), "veto": veto, "over": over, "over_ema": over_ema, "trend_ok": price > e55,
            "above_e20": price > e20, "notes": notes}


# --------------------------------------------------------------------------
# Gate, levels, confidence
# --------------------------------------------------------------------------
def build_levels(price, ev):
    atr1, atr4 = ev["atr1"], ev["atr4"]
    lo, hi = price - 0.3 * atr1, price
    mid = (lo + hi) / 2
    cands = [(p, lab) for p, lab in ev["stops"] if price - 4.0 * atr4 <= p <= price - 1.0 * atr4]
    if cands:
        stop, stop_lab = max(cands, key=lambda x: x[0])
    else:
        stop, stop_lab = price - 2.0 * atr4, "ATR-based"
    risk = mid - stop
    if risk <= 0:
        return None, "invalid stop"
    risk_pct = risk / mid * 100
    if risk_pct > MAX_RISK_PCT:
        return None, f"stop too far ({risk_pct:.1f}%)"
    if ev["resist"] and (ev["resist"][0] - mid) < MIN_RR * risk:
        return None, "resistance too close to entry"
    tc = sorted((p, lab) for p, lab in ev["targets"] if p > mid)

    def pick(lo_r, hi_r, fallback_r, fb_lab):
        """first level between lo_r and hi_r risk-multiples, else a plain R-multiple"""
        for p, lab in tc:
            if lo_r * risk <= p - mid <= hi_r * risk:
                return p, lab
        return mid + fallback_r * risk, fb_lab

    tp1 = pick(MIN_RR, 3.0, MIN_RR, f"{MIN_RR:g}R")
    d1_ = tp1[0] - mid
    r2 = max(d1_ / risk + 0.5, 2.5)
    tp2 = pick(r2, 5.0, r2, f"{r2:.1f}R")
    r3 = max((tp2[0] - mid) / risk + 0.5, 4.0)
    tp3 = pick(r3, 10.0, r3, f"{r3:.1f}R")
    assert stop < lo < hi < tp1[0] < tp2[0] < tp3[0]
    return {"lo": lo, "hi": hi, "mid": mid, "stop": stop, "stop_lab": stop_lab, "risk": risk,
            "risk_pct": risk_pct, "tps": [tp1, tp2, tp3], "rr1": d1_ / risk,
            "chase": hi + 0.5 * atr1}, None


def decide(base, price, ev, ctx):
    votes = ev["votes"]
    pos, neg = {}, 0.0
    for v in votes:
        if v.w > 0:
            pos[v.layer] = pos.get(v.layer, 0) + v.w
        else:
            neg += v.w
    score = sum(min(LCAP.get(l, 99), p) for l, p in pos.items()) + neg
    layers = [l for l in range(1, 6) if pos.get(l, 0) >= 0.5]
    tag = f"{base} score={score:.2f}/{MAX_SCORE:.1f} layers={len(layers)}"
    names = {v.name for v in votes if v.w > 0}

    if REQUIRE_GATE:
        if score < MIN_SCORE:
            return None, f"{tag} score below {MIN_SCORE:g}"
        if len(layers) < MIN_LAYERS:
            return None, f"{tag} too few layers"
        vol_ok = pos.get(4, 0) >= 0.5
        pat_pts = pos.get(3, 0)
        if ev["trend_ok"]:
            route = "Trend"
        elif (pat_pts >= 1.0 or "rsi_div" in names) and vol_ok and ev["above_e20"]:
            route = "Reversal"
        else:
            return None, f"{tag} below 4H EMA55 and no confirmed reversal"
        if (pat_pts >= 1.0 or route == "Reversal") and not vol_ok:
            return None, f"{tag} breakout without volume confirmation"
        if ev["veto"]:
            return None, f"{tag} bearish pattern: {ev['veto'][0]}"
        if ev["over"]:
            return None, f"{tag} not chasing: {ev['over']}"
        if ev["over_ema"] and pat_pts < 0.9:  # pattern breakouts are already limited to 2.5 ATR above the level
            return None, f"{tag} not chasing: {ev['over_ema']}"
        if base != "BTC" and ctx.get("btc_shock") == -1:
            return None, f"{tag} BTC dumping (shock)"
    else:
        route = "Trend" if ev["trend_ok"] else "Reversal"
    lv, why = build_levels(price, ev)
    if lv is None:
        return None, f"{tag} {why}"
    conf = 35 + 3.0 * score + 3 * len(layers) + (0 if route == "Trend" else -4)
    conf += -4 if ctx.get("btc_up") is False else 0
    conf = int(max(35, min(92, round(conf))))
    tier = "A (Very strong)" if score >= 9 else "B (Strong)" if score >= 7.5 else "C (Moderate)"
    return {"base": base, "sym": base + "USDT", "price": price, "score": score, "layers": len(layers),
            "layer_names": [LAYER_NAMES[l] for l in layers], "route": route, "conf": conf,
            "tier": tier, "levels": lv, "for": sorted([v for v in votes if v.w > 0], key=lambda v: -v.w),
            "against": [v for v in votes if v.w < 0]}, tag + " -> SIGNAL"


# --------------------------------------------------------------------------
# Telegram message
# --------------------------------------------------------------------------
def fg_label(v):
    return ("Extreme fear" if v <= 25 else "Fear" if v <= 45 else "Neutral" if v <= 55
            else "Greed" if v <= 75 else "Extreme greed")


def format_signal(sig, ctx):
    e = html.escape
    lv = sig["levels"]
    mid = lv["mid"]
    pct = lambda p: f"{(p / mid - 1) * 100:+.1f}%"
    why = "\n".join(f"• {e(v.text)}" for v in sig["for"][:8])
    more = len(sig["for"]) - 8
    if more > 0:
        why += f"\n• (+{more} more confirmations)"
    mk = []
    if ctx.get("btc_up") is not None:
        mk.append("BTC 4H trend " + ("up" if ctx["btc_up"] else "down"))
    if ctx.get("fg") is not None:
        mk.append(f"Fear&Greed {ctx['fg']} ({fg_label(ctx['fg'])})")
    if ctx.get("stable7") is not None:
        mk.append(f"stablecoin supply {ctx['stable7']:+.2%}/7d")
    size = min(33.0, 100.0 / lv["risk_pct"])
    lines = [
        f"🟢 <b>BUY SIGNAL — {e(sig['base'])}/USDT</b> (Spot)",
        "",
        f"<b>Score:</b> {sig['score']:.1f}/{MAX_SCORE:.1f} · Tier {e(sig['tier'])}",
        f"<b>Confidence:</b> {sig['conf']}% · <b>Route:</b> {sig['route']}",
        f"<b>Layers:</b> {sig['layers']}/5 ({e(', '.join(sig['layer_names']))})",
        "",
        f"<b>Entry zone:</b> <code>{fp(lv['lo'])} – {fp(lv['hi'])}</code>",
        f"<i>Don't chase: skip if price is already above {fp(lv['chase'])}</i>",
        f"<b>Stop-loss:</b> <code>{fp(lv['stop'])}</code> ({pct(lv['stop'])}, {e(lv['stop_lab'])})",
    ]
    for i, (p, lab) in enumerate(lv["tps"], 1):
        lines.append(f"<b>TP{i}:</b> <code>{fp(p)}</code> ({pct(p)}, {e(lab)})")
    lines += [
        f"<b>Risk/Reward to TP1:</b> {lv['rr1']:.1f}",
        f"<b>Suggested size:</b> up to ~{size:.0f}% of capital (risks ~1% if stop hits)",
        "<b>Plan:</b> sell ~1/3 at each TP; after TP1 move stop to entry.",
        "",
        "<b>Why:</b>",
        why,
    ]
    if sig["against"]:
        lines.append("<b>Cautions:</b> " + e("; ".join(v.text for v in sig["against"])))
    if mk:
        lines.append("<b>Market:</b> " + e(" · ".join(mk)))
    lines += ["", "⚠️ Not financial advice. Spot can still lose money; use the stop."]
    return "\n".join(lines)


def chunk_message(text, limit=4096):
    if len(text) <= limit:
        return [text]
    parts, cur = [], ""
    for line in text.split("\n"):
        while len(line) > limit:
            parts.append(line[:limit])
            line = line[limit:]
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
        r = None
        for attempt in range(3):
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
# Binance Square post (2-4 lines: coin, why it was chosen, the bot's view; no TP/SL)
# --------------------------------------------------------------------------
SQUARE_URL = "https://www.binance.com/bapi/composite/v1/public/pgc/openApi/content/add"
SQUARE_HALT = {"reason": None}  # set when the key is bad / daily limit reached -> stop posting this run


def _join(items):
    if not items:
        return "several indicators lining up"
    return items[0] if len(items) == 1 else ", ".join(items[:-1]) + " and " + items[-1]


def _pick_reasons(sig, k=3):
    """Strongest vote of each different layer first, so the reasons do not repeat each other."""
    out, seen = [], set()
    for v in sig["for"]:
        if v.layer in seen or v.layer == 6:
            continue
        seen.add(v.layer)
        out.append(SHORT.get(v.name, v.text))
        if len(out) == k:
            break
    for v in sig["for"]:
        if len(out) >= 2:
            break
        t = SHORT.get(v.name, v.text)
        if t not in out:
            out.append(t)
    return out


def _view(sig, ctx):
    conv = "strong" if sig["tier"].startswith("A") else "solid" if sig["tier"].startswith("B") else "moderate"
    core = ("trend continuation with buyers in control and room to run before major resistance"
            if sig["route"] == "Trend" else
            "early reversal attempt that still needs follow-through")
    text = f"{conv} bullish setup, {core} ({sig['conf']}% confidence)"
    cautions = []
    if ctx.get("btc_up") is False:
        cautions.append("BTC is weak")
    if (ctx.get("fg") or 0) >= 75:
        cautions.append("the market is greedy")
    if cautions:
        text += f" - but {' and '.join(cautions)}, so stay careful"
    return text + "."


def square_text(sig, ctx):
    """Exactly 4 short lines: coin name, why chosen, my view, hashtags. No entry/TP/SL numbers."""
    b = sig["base"]
    heads = [f"${b} is on my radar 👀", f"Watching ${b} 🟢", f"${b} just lit up my scanner ✅"]
    head = heads[zlib.crc32(f"{b}{time.strftime('%Y%m%d')}".encode()) % len(heads)]
    return "\n".join([
        head,
        f"Why: {_join(_pick_reasons(sig))}.",
        f"My view: {_view(sig, ctx)}",
        f"#{b} #Binance #Crypto | Not financial advice",
    ])


def square_post(text):
    """Post to Binance Square. Returns (ok, info). Never raises, never logs the key."""
    key = os.getenv("BINANCE_SQUARE_OPENAPI_KEY", "").strip()
    if not key:
        return False, "no BINANCE_SQUARE_OPENAPI_KEY set"
    if SQUARE_HALT["reason"]:
        return False, "posting halted: " + SQUARE_HALT["reason"]
    try:
        r = requests.post(SQUARE_URL, json={"bodyTextOnly": text}, timeout=25,
                          headers={"X-Square-OpenAPI-Key": key, "Content-Type": "application/json",
                                   "clienttype": "binanceSkill"})
    except requests.RequestException as e:  # no retry: a retry after a timeout could post twice
        return False, "network error: " + str(e).replace(key, "***")[:150]
    if r.status_code == 504:  # Binance can answer 504 even though the post went through
        return True, "submitted (504 timeout, Binance may still have published it)"
    try:
        j = r.json()
    except ValueError:
        return False, f"HTTP {r.status_code}, unreadable reply"
    code, msg = str(j.get("code")), j.get("message")
    if code == "000000":
        pid = (j.get("data") or {}).get("id")
        return True, f"https://www.binance.com/square/post/{pid}" if pid else "posted"
    if code in ("220009", "220003", "220004", "10005"):
        SQUARE_HALT["reason"] = f"{code} {msg}"
    return False, f"{code} {msg}"


# --------------------------------------------------------------------------
# State, dedup, signal tracker (forward track record)
# --------------------------------------------------------------------------
def new_state():
    return {"last": {}, "open": [], "stats": {"win": 0, "loss": 0, "expired": 0, "tp1": 0, "tp2": 0, "tp3": 0}}


def load_state():
    try:
        with open(STATE_FILE) as f:
            s = json.load(f)
        base = new_state()
        base.update(s)
        base["stats"] = {**new_state()["stats"], **s.get("stats", {})}
        return base
    except Exception:
        return new_state()


def save_state(state):
    cutoff = time.time() - 96 * 3600
    state["last"] = {k: v for k, v in state.get("last", {}).items() if v["ts"] >= cutoff}
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def is_duplicate(state, sig, now):
    if any(p["sym"] == sig["sym"] for p in state.get("open", [])):
        return True
    prev = state.get("last", {}).get(sig["sym"])
    if not prev:
        return False
    age_h = (now - prev["ts"]) / 3600
    if age_h < COOLDOWN_H:
        return True
    if age_h >= HARD_EXPIRY_H:
        return False
    return abs(sig["price"] - prev["price"]) < 0.75 * prev["tp1_dist"]


def resolve(p, df):
    """Replay 1H candles after the signal. Stop is hit first if a candle touches both."""
    events, hit = [], 0
    t0 = p["ts"] * 1000
    for t, h, l in zip(df.t.values, df.h.values, df.l.values):
        if t < t0:
            continue  # only candles that opened after the signal
        stop = p["stop"] if hit == 0 else p["entry"]
        if l <= stop:
            events.append("sl" if hit == 0 else "be")
            return events, True
        while hit < 3 and h >= p["tps"][hit]:
            hit += 1
            events.append(f"tp{hit}")
        if hit == 3:
            return events, True
    return events, False


def tally(stats):
    n = stats["win"] + stats["loss"]
    return f"{stats['win']} wins / {stats['loss']} losses" + (f" ({stats['win'] / n:.0%})" if n else "")


def track_open(ex, state, now):
    msgs, keep = [], []
    for p in state.get("open", []):
        try:
            df = ex.klines(p["sym"], "1h", 200)
        except Exception as e:
            log.warning("tracker: %s fetch failed (%s)", p["sym"], e)
            keep.append(p)
            continue
        events, closed = resolve(p, df)
        new = events[len(p.get("events", [])):]
        base = p["sym"][:-4]
        st = state["stats"]
        for ev in new:
            if ev.startswith("tp"):
                st[ev] += 1
                gain = (p["tps"][int(ev[2]) - 1] / p["entry"] - 1) * 100
                msgs.append(f"✅ <b>{html.escape(base)}/USDT {ev.upper()} hit</b> ({gain:+.1f}%)"
                            + ("\nMove stop to entry." if ev == "tp1" else ""))
            elif ev == "sl":
                st["loss"] += 1
                msgs.append(f"❌ <b>{html.escape(base)}/USDT stop-loss hit</b> "
                            f"({(p['stop'] / p['entry'] - 1) * 100:+.1f}%)")
            elif ev == "be":
                msgs.append(f"➖ <b>{html.escape(base)}/USDT back to entry after TP1</b> (closed ~0%)")
        p["events"] = events
        age_d = (now - p["ts"]) / 86400
        if closed:
            if events and events[-1] in ("tp3", "be"):
                st["win"] += 1
        elif age_d >= TRACK_DAYS:
            closed = True
            st["win" if events else "expired"] += 1
            msgs.append(f"⌛ <b>{html.escape(base)}/USDT tracking ended</b> after {TRACK_DAYS:g} days"
                        + (f" (reached {events[-1].upper()})" if events else " (no TP or SL hit)"))
        if new:
            msgs[-1] += f"\n<i>Track record: {tally(st)}</i>"
        if not closed:
            keep.append(p)
    state["open"] = keep
    return msgs


# --------------------------------------------------------------------------
# Scan
# --------------------------------------------------------------------------
def prescreen(d4):
    """Cheap 4H check, so 1H/1D candles are only downloaded for coins that can qualify."""
    c = d4.c
    if float(c.iloc[-1]) > float(ema(c, 55).iloc[-1]):
        return True
    a = float(atr(d4).iloc[-1])
    if not a > 0:
        return False
    for _, fn in PATTERNS:
        try:
            if fn(d4, a):
                return True
        except Exception:
            pass
    return rsi_bull_div(d4)


def _near_miss(why):
    try:
        return float(why.split("score=")[1].split("/")[0]) >= 0.6 * MIN_SCORE
    except Exception:
        return False


def analyze(ex, base, ctx):
    if time.time() > ctx.get("deadline", 1e18):
        return None
    sym = base + "USDT"
    try:
        d4 = ex.klines(sym, "4h", 300)
        if len(d4) < 120:
            return None
        if not prescreen(d4.iloc[:-1].reset_index(drop=True)):
            return None
        d1 = ex.klines(sym, "1h", 250)
        dd = ex.klines(sym, "1d", 200)
    except Exception as e:
        log.warning("%s: data fetch failed (%s)", base, e)
        return None
    if len(d1) < 120:
        return None
    price = float(d1.c.iloc[-1])
    d1, d4, dd = (d.iloc[:-1].reset_index(drop=True) for d in (d1, d4, dd))  # closed candles only
    try:
        ev = evaluate(base, price, d1, d4, dd, ctx)
        if ev is None:
            return None
        sig, why = decide(base, price, ev, ctx)
    except Exception as e:
        log.exception("%s: analysis error: %s", base, e)
        return None
    (log.info if sig or _near_miss(why) else log.debug)(why)
    return sig


def scan(dry=False, ex=None, now=None):
    ex = ex or Spot(make_session())
    now = now or time.time()
    state = load_state()
    try:
        coins = ex.universe()
        if not coins:
            raise RuntimeError("empty universe")
    except Exception as e:
        log.warning("universe fetch failed (%s), using fallback list", e)
        coins = FALLBACK
    log.info("scanning %d coins", len(coins))
    ctx = get_context(getattr(ex, "s", make_session()), ex, coins)
    ctx["deadline"] = time.time() + SCAN_BUDGET_S
    log.info("context: BTC up=%s shock=%s | F&G=%s | stable7=%s | chains=%d",
             ctx["btc_up"], ctx["btc_shock"], ctx["fg"], ctx["stable7"], len(ctx["chain"]))
    for m in track_open(ex, state, now):
        if dry:
            print(m, "\n" + "-" * 40)
        else:
            tg_send(m)
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        results = list(pool.map(lambda b: analyze(ex, b, ctx), coins))
    if time.time() > ctx["deadline"]:
        log.warning("scan budget of %ss reached - some coins were skipped this run", SCAN_BUDGET_S)
    sigs = sorted([s for s in results if s], key=lambda s: (-s["score"], -s["conf"]))
    sent, sq_last, sq_alerted = 0, 0.0, False
    use_square = bool(os.getenv("BINANCE_SQUARE_OPENAPI_KEY", "").strip())
    for s in sigs:
        if is_duplicate(state, s, now):
            log.info("%s suppressed (open / cooldown / price not moved)", s["base"])
            continue
        if sent >= MAX_SIGNALS:
            break
        msg = format_signal(s, ctx)
        sq = square_text(s, ctx)
        lv = s["levels"]
        if dry:
            print(msg, "\n" + "-" * 40)
            print("[Binance Square preview]\n" + sq, "\n" + "-" * 40)
        else:
            tg_send(msg)
            state["last"][s["sym"]] = {"ts": now, "price": s["price"], "tp1_dist": lv["tps"][0][0] - lv["mid"]}
            state["open"].append({"sym": s["sym"], "ts": now, "entry": lv["mid"], "stop": lv["stop"],
                                  "tps": [t[0] for t in lv["tps"]], "events": []})
            if use_square and not SQUARE_HALT["reason"]:
                wait = SQUARE_GAP_S - (time.time() - sq_last)
                if sq_last and wait > 0:
                    time.sleep(wait)  # 10-15 s between Square posts
                ok_, info = square_post(sq)
                sq_last = time.time()
                log.info("Square post %s: %s", s["base"], info if ok_ else "FAILED - " + str(info))
                if SQUARE_HALT["reason"] and not sq_alerted:
                    sq_alerted = True
                    try:
                        tg_send("⚠️ Binance Square posting stopped: " + html.escape(SQUARE_HALT["reason"]))
                    except Exception:
                        pass
        sent += 1
    if not dry:
        save_state(state)
    log.info("scan done: %d candidates, %d sent | track record: %s", len(sigs), sent, tally(state["stats"]))
    return sent


# --------------------------------------------------------------------------
# Offline self-test (synthetic data, no network)
# --------------------------------------------------------------------------
def _ohlc(close, noise=0.0, seed=0, vol=None):
    rng = np.random.default_rng(seed)
    c = np.asarray(close, float) * (1 + rng.normal(0, noise, len(close))) if noise else np.asarray(close, float)
    o = np.concatenate([[c[0]], c[:-1]])
    wick = np.abs(rng.normal(0, 0.002, len(c))) * c + 0.0005 * c
    v = np.full(len(c), 100.0) if vol is None else np.asarray(vol, float)
    return pd.DataFrame({"t": np.arange(len(c)) * 14_400_000.0, "o": o, "h": np.maximum(o, c) + wick,
                         "l": np.minimum(o, c) - wick, "c": c, "v": v, "tb": v * 0.5})


def _path(points):
    xs, ys = zip(*points)
    return np.interp(np.arange(xs[0], xs[-1] + 1), xs, ys)


def _walk(n, seed, drift=0.0, vol=0.01, start=100.0):
    rng = np.random.default_rng(seed)
    return start * np.exp(np.cumsum(rng.normal(drift, vol, n)))


def _cup():
    left = _path([(0, 80), (15, 100)])
    x = np.arange(16, 106)
    cup = 80 + 20 * ((x - 60.5) / 44.5) ** 2
    cup = np.minimum(cup, 100)
    handle = _path([(105, 100), (112, 95), (118, 102)])[1:]
    return np.concatenate([left, cup, handle])


CASES = {
    "ihs": _path([(0, 105), (20, 90), (35, 98), (50, 82), (65, 98), (80, 90), (90, 100)]),
    "dbl_bottom": _path([(0, 110), (20, 90), (40, 104), (60, 90.5), (72, 98), (80, 106)]),
    "cup_handle": _cup(),
    "triangle": _path([(0, 90), (10, 100), (20, 85), (30, 100), (40, 90), (50, 100), (60, 94),
                       (70, 100), (75, 97), (85, 102)]),
    "trend_break": _path([(0, 90), (12, 110), (24, 96), (36, 104), (46, 95), (58, 100), (64, 96), (70, 100)]),
    "bull_flag": _path([(0, 100), (20, 100), (30, 115), (40, 111), (41, 118)]),
}


def selftest():
    global MIN_SCORE, MIN_LAYERS, REQUIRE_GATE, MAX_RISK_PCT
    ok = True
    print(f"MAX_SCORE = {MAX_SCORE:.2f}, weights ok = {set(W) == set(LAYER)}")
    # 1) each detector fires on its crafted pattern
    fn = dict(PATTERNS)
    for key, series in CASES.items():
        df = _ohlc(series, noise=0.0, seed=1)
        a = float(atr(df).iloc[-1])
        res = fn[key](df, a)
        print(f"  {key:11s}: {'FOUND ' + res['name'] if res else 'not found'}")
        ok = ok and bool(res)
    # bearish veto
    top = _path([(0, 90), (20, 110), (35, 100), (50, 118), (65, 100), (80, 110), (92, 96)])
    dft = _ohlc(top)
    veto = bearish_patterns(dft, float(atr(dft).iloc[-1]))
    print("  H&S top veto:", veto)
    ok = ok and bool(veto)
    # FVG: strong green candle leaves a gap, then price retests it
    base_ = list(np.linspace(100, 101, 40))
    rows = []
    for i, cl in enumerate(base_):
        rows.append((cl, cl + 0.3, cl - 0.3, cl))
    rows += [(101, 101.2, 100.8, 101.0), (101.0, 105.5, 100.9, 105.0), (105.0, 107.0, 103.0, 106.5),
             (106.5, 107.0, 105.5, 106.0), (106.0, 106.2, 103.4, 103.8), (103.8, 105.0, 103.2, 104.8)]
    arr = np.array(rows)
    dfv = pd.DataFrame({"t": np.arange(len(arr)) * 3.6e6, "o": arr[:, 0], "h": arr[:, 1], "l": arr[:, 2],
                        "c": arr[:, 3], "v": 100.0, "tb": 50.0})
    # ensure gap: candle -5 high (101.2) < candle -3 low (103.0)
    z = bull_fvg(dfv, float(atr(dfv).iloc[-1]), 30)
    print("  FVG:", z)
    ok = ok and bool(z)
    # fib: leg 80->100, retrace to ~88 (r=0.6), then bullish 1H candle
    fibp = _path([(0, 95), (15, 80), (45, 100), (70, 88), (80, 89)])
    d4f = _ohlc(fibp)
    d1f = _ohlc(np.full(60, 88.0))
    d1f.loc[d1f.index[-1], ["o", "c"]] = [87.8, 88.4]
    fb = fib_setup(d4f, d1f, 89.0, float(atr(d4f).iloc[-1]))
    print("  Fibonacci:", None if not fb else f"retrace {fb['r']:.3f}")
    ok = ok and bool(fb)

    # 2) false-positive rate of each detector on random walks
    N = 400
    cnt = {k: 0 for k, _ in PATTERNS}
    cnt["bear"] = 0
    cnt["fvg"] = 0
    cnt["fib"] = 0
    for sd in range(N):
        d = _ohlc(_walk(300, sd, vol=0.012), noise=0.0, seed=sd)
        a = float(atr(d).iloc[-1])
        for k, f in PATTERNS:
            try:
                cnt[k] += 1 if f(d, a) else 0
            except Exception as e:
                print("  detector error", k, e)
                ok = False
        cnt["bear"] += 1 if bearish_patterns(d, a) else 0
        cnt["fvg"] += 1 if bull_fvg(d, a, 30) else 0
        cnt["fib"] += 1 if fib_setup(d, d, float(d.c.iloc[-1]), a) else 0
    print("  false positives on random walks (%):", {k: round(100 * v / N, 1) for k, v in cnt.items()})

    # 3) full pipeline on random data: strict gate vs relaxed gate (levels, format, order)
    class Fake:
        name = "fake"

        def klines(self, sym, interval, limit):
            seed = zlib.crc32(f"{sym}{interval}".encode())
            vol = {"1h": 0.006, "4h": 0.012, "1d": 0.03}[interval]
            return _ohlc(_walk(limit, seed, drift=((seed % 7) - 3) * 0.0003, vol=vol, start=50 + seed % 200),
                         seed=seed, vol=np.random.default_rng(seed).lognormal(5, 0.5, limit))

    ctx = {"fg": 50, "stable7": 0.004, "btc_up": True, "btc_shock": 0, "btc_chg4h": 0.0,
           "chain": {"Solana": 0.05}}
    coins = FALLBACK
    strict = [analyze(Fake(), b, ctx) for b in coins]
    print(f"  strict gate on random data: {sum(1 for s in strict if s)}/{len(coins)} signals")
    MIN_SCORE, MIN_LAYERS, MAX_RISK_PCT, REQUIRE_GATE = 1.5, 1, 20.0, False
    relaxed = [s for s in (analyze(Fake(), b, ctx) for b in coins) if s]
    print(f"  relaxed gate: {len(relaxed)}/{len(coins)} signals")
    if not relaxed:
        print("FAIL: no signal even with relaxed gate")
        return 1
    for s in relaxed:
        lv = s["levels"]
        tp = [t[0] for t in lv["tps"]]
        if not (lv["stop"] < lv["lo"] < lv["hi"] < tp[0] < tp[1] < tp[2]):
            ok = False
            print("FAIL level order", s["base"], lv)
        if lv["rr1"] < MIN_RR - 1e-9:
            ok = False
            print("FAIL rr", s["base"], lv["rr1"])
    print(format_signal(relaxed[0], ctx))

    # 4) dedup + tracker
    st = new_state()
    s0 = relaxed[0]
    now = time.time()
    st["last"][s0["sym"]] = {"ts": now - 3600, "price": s0["price"], "tp1_dist": s0["levels"]["tps"][0][0] - s0["levels"]["mid"]}
    d_a = is_duplicate(st, s0, now)
    st["last"][s0["sym"]]["ts"] = now - 20 * 3600
    d_b = is_duplicate(st, s0, now)
    far = dict(s0, price=s0["price"] * 1.5)
    d_c = is_duplicate(st, far, now)
    st["open"].append({"sym": s0["sym"], "ts": now})
    d_d = is_duplicate(st, far, now)
    print(f"  dedup: cooldown={d_a} (T) stale-repeat={d_b} (T) moved={d_c} (F) still-open={d_d} (T)")
    ok = ok and d_a and d_b and not d_c and d_d
    pos = {"ts": 1000, "entry": 100.0, "stop": 96.0, "tps": [106.0, 110.0, 116.0]}

    def candles(rows):
        return pd.DataFrame({"t": [(1000 + 3600 * (i + 1)) * 1000.0 for i in range(len(rows))],
                             "h": [r[0] for r in rows], "l": [r[1] for r in rows]})
    cases = [("win all TPs", [(107, 99), (111, 101), (117, 108)], ["tp1", "tp2", "tp3"], True),
             ("SL first", [(101, 95), (110, 100)], ["sl"], True),
             ("TP1 then back to entry", [(107, 99), (105, 99.9)], ["tp1", "be"], True),
             ("still open", [(103, 99)], [], False),
             ("SL+TP same candle -> SL", [(107, 95)], ["sl"], True)]
    for name, rows, want, closed_want in cases:
        ev, cl = resolve(pos, candles(rows))
        good = ev == want and cl == closed_want
        ok = ok and good
        print(f"  tracker {name}: {ev} closed={cl} {'ok' if good else 'FAIL'}")
    # 5) universe: every USDT pair, no price cap, stocks/stables/illiquid removed
    class U(Spot):
        def __init__(self):
            self.s, self.base = None, None

        def _get(self, path, **p):
            row = lambda sym, qv, last, chg="3": {"symbol": sym, "quoteVolume": qv, "lastPrice": last,
                                                  "priceChangePercent": chg}
            return [row("BTCUSDT", "5e9", "95000"), row("PEPEUSDT", "2e6", "0.0000123"),
                    row("SHIBUSDT", "3e7", "0.00001"), row("USDCUSDT", "9e8", "1.0001", "0.01"),
                    row("TSLAXUSDT", "4e6", "250"), row("NVDAONUSDT", "4e6", "180"),
                    row("AAPLUSDT", "4e6", "230"), row("AVAXUSDT", "9e6", "30"),
                    row("DEADUSDT", "1000", "5"), row("ETHBTC", "9e9", "0.03")]
    uni = U().universe()
    print("  universe:", uni)
    ok = ok and uni == ["BTC", "SHIB", "AVAX", "PEPE"] and not any(is_stock_token(b) for b in uni)
    ok = ok and is_stock_token("TSLAX") and is_stock_token("AAPLON") and not is_stock_token("AVAX") \
        and not is_stock_token("PEPE") and not is_stock_token("ONDO") and not is_stock_token("MAX")
    # 6) two-stage scan downloads 1H/1D only for coins that pass the 4H prescreen
    calls = {"1h": 0, "4h": 0, "1d": 0}

    class Counting(Fake):
        def klines(self, sym, interval, limit):
            calls[interval] += 1
            return super().klines(sym, interval, limit)
    for b in FALLBACK:
        analyze(Counting(), b, dict(ctx))
    print("  api calls per interval:", calls)
    ok = ok and calls["4h"] == len(FALLBACK) and calls["1h"] < calls["4h"] and calls["1h"] == calls["1d"]
    # 7) Binance Square post: 4 short lines, coin + why + view, no TP/SL numbers
    bad = ("TP", "SL", "stop", "Stop", "Entry", "entry", "R/R", "target")
    for sg in relaxed[:12]:
        t = square_text(sg, ctx)
        good = (len(t.split("\n")) <= 4 and f"${sg['base']}" in t and "Why:" in t and "My view:" in t
                and not any(w in t for w in bad) and len(t) < 420)
        if not good:
            ok = False
            print("FAIL square text:", repr(t))
    sample = dict(relaxed[0], tier="B (Strong)", route="Trend", conf=74)
    print("  --- sample Square post ---")
    print("  " + square_text(sample, ctx).replace("\n", "\n  "))
    weak = square_text(dict(sample, route="Reversal", tier="C (Moderate)", conf=58), dict(ctx, btc_up=False, fg=82))
    ok = ok and "BTC is weak" in weak and "greedy" in weak and "reversal" in weak
    seen = {}

    class Resp:
        def __init__(self, status, body):
            self.status_code, self._b = status, body

        def json(self):
            if self._b is None:
                raise ValueError
            return self._b
    real_post = requests.post
    os.environ["BINANCE_SQUARE_OPENAPI_KEY"] = "SECRETKEY123"
    SQUARE_HALT["reason"] = None
    try:
        def fake_ok(url, **kw):
            seen.update(kw, url=url)
            return Resp(200, {"code": "000000", "message": None, "data": {"id": "777"}})
        requests.post = fake_ok
        r1 = square_post("hello")
        sent_ok = (seen["url"] == SQUARE_URL and seen["json"] == {"bodyTextOnly": "hello"}
                   and seen["headers"]["X-Square-OpenAPI-Key"] == "SECRETKEY123"
                   and seen["headers"]["clienttype"] == "binanceSkill")
        requests.post = lambda url, **kw: Resp(504, None)
        r2 = square_post("x")
        requests.post = lambda url, **kw: Resp(200, {"code": "220009", "message": "limit"})
        r3 = square_post("x")
        requests.post = fake_ok
        r4 = square_post("x")  # must be blocked after the daily-limit error
        SQUARE_HALT["reason"] = None

        def boom(url, **kw):
            raise requests.ConnectionError("failed for SECRETKEY123")
        requests.post = boom
        r5 = square_post("x")
        requests.post = lambda url, **kw: Resp(200, {"code": "20002", "message": "sensitive"})
        r6 = square_post("x")
        halted_by_sensitive = SQUARE_HALT["reason"] is not None
        os.environ["BINANCE_SQUARE_OPENAPI_KEY"] = ""
        r7 = square_post("x")
    finally:
        requests.post = real_post
        SQUARE_HALT["reason"] = None
        os.environ.pop("BINANCE_SQUARE_OPENAPI_KEY", None)
    print("  square api:", r1, "|", r2[0], "|", r3, "|", r4, "|", r5, "|", r6, "|", r7)
    ok = (ok and sent_ok and r1 == (True, "https://www.binance.com/square/post/777") and r2[0] is True
          and r3[0] is False and r4[0] is False and "halted" in r4[1]
          and r5[0] is False and "SECRETKEY123" not in r5[1] and r6[0] is False
          and not halted_by_sensitive and r7[0] is False)
    ch = chunk_message("\n".join(["x" * 100] * 120))
    ok = ok and max(map(len, ch)) <= 4096
    print("SELFTEST", "OK" if ok else "FAILED")
    return 0 if ok else 1


# --------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--scan", action="store_true")
    g.add_argument("--test", action="store_true")
    g.add_argument("--dry-run", action="store_true")
    g.add_argument("--stats", action="store_true")
    g.add_argument("--selftest", action="store_true")
    g.add_argument("--square-test", action="store_true")
    a = ap.parse_args()
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(message)s")
    if a.selftest:
        sys.exit(selftest())
    if a.stats:
        s = load_state()
        print("track record:", tally(s["stats"]), s["stats"], "| open:", [p["sym"] for p in s["open"]])
        return
    if a.square_test:
        ok_, info = square_post("Testing my crypto scanner 🤖 #Binance #Crypto")
        print("square test:", "OK -> " + str(info) if ok_ else "FAILED -> " + str(info))
        print("(this created a real public post if OK - you can delete it on Binance Square)")
        return
    if a.test:
        tg_send("✅ Spot signal bot connected. BUY signals will appear here when a setup clears the gate.")
        print("test message sent")
        return
    scan(dry=a.dry_run)


if __name__ == "__main__":
    main()
