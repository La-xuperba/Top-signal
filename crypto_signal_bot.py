"""
BINANCE GEMS — Pre-pump Binance Spot Screener + Telegram/Square posting
----------------------------------------------------------------------
Install: pip install requests pandas numpy

Environment variables (GitHub Actions Secrets):
  TELEGRAM_BOT_TOKEN
  TELEGRAM_CHAT_ID
  BINANCE_SQUARE_API_URL  (optional; only if you have a documented/authorized API endpoint)
  BINANCE_SQUARE_API_TOKEN (optional; token for your own authorized posting service)

Important:
- Binance Spot public market data is used for screening.
- Telegram posting uses the official Telegram Bot API.
- Binance Square automatic posting is attempted only through a user-configured,
  documented/authorized endpoint. No fake or undocumented Binance endpoint is assumed.
- Without that endpoint, Square text is saved to square_drafts.txt for manual posting.
- This is a candidate screener, not financial advice or an order-execution bot.
"""

import os
import time
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import numpy as np
import pandas as pd

BASE_URL = "https://api.binance.com"
QUOTE_ASSET = "USDT"

MIN_24H_QUOTE_VOLUME = 15_000_000
MIN_PRICE = 0.01
MAX_24H_CHANGE_PCT = 15.0

TREND_INTERVAL = "1h"
TRIGGER_INTERVAL = "15m"
TREND_LIMIT = 120
TRIGGER_LIMIT = 100

BTC_RSI_MIN = 48
COIN_RSI_MIN = 52
COIN_RSI_MAX = 67
MIN_VOLUME_RATIO = 2.0
PREVIOUS_VOLUME_RATIO = 1.5
MIN_DISTANCE_FROM_EMA20 = 1.01
MAX_DISTANCE_FROM_EMA20 = 1.06
MAX_UPPER_WICK_RATIO = 1.5
MIN_CANDLE_BODY_ATR_RATIO = 0.4

MIN_SCORE = 75
TOP_N = 5
MAX_WORKERS = 8
REQUEST_TIMEOUT = 12

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
SQUARE_API_URL = os.getenv("BINANCE_SQUARE_API_URL", "").strip()
SQUARE_API_TOKEN = os.getenv("BINANCE_SQUARE_API_TOKEN", "").strip()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("binance_gems")


def binance_get(endpoint, params=None):
    response = requests.get(
        f"{BASE_URL}{endpoint}", params=params, timeout=REQUEST_TIMEOUT
    )
    response.raise_for_status()
    return response.json()


def get_klines(symbol, interval, limit):
    raw = binance_get(
        "/api/v3/klines",
        {"symbol": symbol, "interval": interval, "limit": limit},
    )
    cols = [
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "number_of_trades",
        "taker_buy_base_volume", "taker_buy_quote_volume", "ignore",
    ]
    df = pd.DataFrame(raw, columns=cols)
    for col in [
        "open", "high", "low", "close", "volume", "quote_volume",
        "number_of_trades", "taker_buy_base_volume", "taker_buy_quote_volume",
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    return df


def ema(series, period):
    return series.ewm(span=period, adjust=False).mean()


def rsi(series, period=14):
    delta = series.diff()
    gains = delta.clip(lower=0)
    losses = -delta.clip(upper=0)
    avg_gain = gains.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = losses.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def atr(df, period=14):
    prev = df["close"].shift(1)
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - prev).abs(),
        (df["low"] - prev).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / period, adjust=False).mean()


def macd_histogram(close, fast=12, slow=26, signal=9):
    macd = ema(close, fast) - ema(close, slow)
    return macd - ema(macd, signal)


def add_1h_indicators(df):
    df = df.copy()
    df["ema20"] = ema(df["close"], 20)
    df["ema50"] = ema(df["close"], 50)
    df["rsi14"] = rsi(df["close"])
    df["return_6h"] = df["close"].pct_change(6) * 100
    return df.dropna().reset_index(drop=True)


def add_15m_indicators(df):
    df = df.copy()
    df["rsi14"] = rsi(df["close"])
    df["atr14"] = atr(df)
    df["macd_histogram"] = macd_histogram(df["close"])
    df["volume_sma20"] = df["volume"].rolling(20).mean()
    df["volume_ratio"] = df["volume"] / df["volume_sma20"]
    df["previous_high_20"] = df["high"].shift(1).rolling(20).max()
    df["candle_body"] = (df["close"] - df["open"]).abs()
    df["upper_wick"] = df["high"] - df[["open", "close"]].max(axis=1)
    df["upper_wick_to_body"] = (
        df["upper_wick"] / df["candle_body"].replace(0, np.nan)
    )
    return df.dropna().reset_index(drop=True)


def build_usdt_universe():
    info = binance_get("/api/v3/exchangeInfo")
    tickers = binance_get("/api/v3/ticker/24hr")
    by_symbol = {t["symbol"]: t for t in tickers}
    stable_bases = {
        "USDC", "FDUSD", "TUSD", "USDP", "DAI", "USDD", "BUSD",
        "EUR", "TRY", "USDE", "PYUSD",
    }
    excluded = ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")
    universe = []
    for item in info["symbols"]:
        symbol = item["symbol"]
        base = item["baseAsset"]
        if item.get("status") != "TRADING":
            continue
        if not item.get("isSpotTradingAllowed", False):
            continue
        if item.get("quoteAsset") != QUOTE_ASSET or base in stable_bases:
            continue
        if any(symbol.endswith(suffix) for suffix in excluded):
            continue
        ticker = by_symbol.get(symbol)
        if not ticker:
            continue
        price = float(ticker["lastPrice"])
        volume = float(ticker["quoteVolume"])
        change = float(ticker["priceChangePercent"])
        if price < MIN_PRICE or volume < MIN_24H_QUOTE_VOLUME:
            continue
        if change >= MAX_24H_CHANGE_PCT:
            continue
        universe.append({
            "symbol": symbol,
            "base_asset": base,
            "last_price": price,
            "quote_volume_24h": volume,
            "price_change_24h_pct": change,
        })
    return universe


def get_btc_market_regime():
    df = add_1h_indicators(get_klines("BTCUSDT", TREND_INTERVAL, TREND_LIMIT))
    row = df.iloc[-2]  # previous fully closed candle
    enabled = (
        row["close"] > row["ema50"]
        and row["ema20"] > row["ema50"]
        and row["rsi14"] >= BTC_RSI_MIN
    )
    return {"enabled": bool(enabled), "return_6h": float(row["return_6h"]),
            "close": float(row["close"]), "rsi14": float(row["rsi14"]),
            "close_time": row["close_time"]}


def calculate_score(trend, trigger, btc_return_6h):
    score, reasons = 0, []
    trend_ok = trend["close"] > trend["ema20"] > trend["ema50"]
    if trend_ok:
        score += 25
        reasons.append("1h trend aligned")
    if trigger["volume_ratio"] >= MIN_VOLUME_RATIO:
        score += 20
        reasons.append(f"15m volume {trigger['volume_ratio']:.2f}x average")
    breakout_ok = trigger["close"] > trigger["previous_high_20"]
    if breakout_ok:
        score += 15
        reasons.append("15m breakout")
    momentum_ok = (
        COIN_RSI_MIN <= trigger["rsi14"] <= COIN_RSI_MAX
        and trigger["macd_histogram"] > 0
    )
    if momentum_ok:
        score += 15
        reasons.append("RSI/MACD aligned")
    relative_strength = trend["return_6h"] - btc_return_6h
    if relative_strength > 0:
        score += 15
        reasons.append(f"outperformed BTC by {relative_strength:.2f}% in 6h")
    distance = trend["close"] / trend["ema20"]
    extension_ok = MIN_DISTANCE_FROM_EMA20 <= distance <= MAX_DISTANCE_FROM_EMA20
    if extension_ok:
        score += 10
        reasons.append("EMA20 extension within range")
    return {
        "score": score, "trend_ok": trend_ok, "breakout_ok": breakout_ok,
        "momentum_ok": momentum_ok, "extension_ok": extension_ok,
        "relative_strength": relative_strength, "distance": distance,
        "reasons": reasons,
    }


def screen_symbol(data, btc_return_6h):
    symbol = data["symbol"]
    trend_df = add_1h_indicators(get_klines(symbol, TREND_INTERVAL, TREND_LIMIT))
    trigger_df = add_15m_indicators(get_klines(symbol, TRIGGER_INTERVAL, TRIGGER_LIMIT))
    if len(trend_df) < 3 or len(trigger_df) < 4:
        raise ValueError("Not enough candles after indicator warm-up")
    trend, trigger = trend_df.iloc[-2], trigger_df.iloc[-2]
    prev_trigger = trigger_df.iloc[-3]

    previous_volume_ok = prev_trigger["volume_ratio"] >= PREVIOUS_VOLUME_RATIO
    current_volume_ok = trigger["volume_ratio"] >= MIN_VOLUME_RATIO
    green = trigger["close"] > trigger["open"]
    body_atr = trigger["candle_body"] / trigger["atr14"] if trigger["atr14"] > 0 else 0
    wick = trigger["upper_wick_to_body"]
    wick_ok = pd.notna(wick) and wick <= MAX_UPPER_WICK_RATIO

    scoring = calculate_score(trend, trigger, btc_return_6h)
    eligible = all([
        scoring["trend_ok"], scoring["breakout_ok"], scoring["momentum_ok"],
        scoring["extension_ok"], current_volume_ok, previous_volume_ok,
        green, body_atr >= MIN_CANDLE_BODY_ATR_RATIO, wick_ok,
        scoring["relative_strength"] > 0, scoring["score"] >= MIN_SCORE,
    ])
    return {
        **data,
        "candle_close_time": trigger["close_time"],
        "eligible": eligible,
        "score": int(scoring["score"]),
        "close": float(trigger["close"]),
        "rsi_15m": float(trigger["rsi14"]),
        "volume_ratio_15m": float(trigger["volume_ratio"]),
        "previous_volume_ratio_15m": float(prev_trigger["volume_ratio"]),
        "relative_strength_vs_btc_pct": float(scoring["relative_strength"]),
        "distance_from_ema20_pct": float((scoring["distance"] - 1) * 100),
        "reasons": " | ".join(scoring["reasons"]),
    }


def scan():
    log.info("Loading BTC market regime...")
    btc = get_btc_market_regime()
    log.info("BTC regime=%s, close=%.2f, RSI=%.1f",
             "ON" if btc["enabled"] else "OFF", btc["close"], btc["rsi14"])
    if not btc["enabled"]:
        log.warning("BTC filter OFF; no candidates selected.")
        return pd.DataFrame()

    universe = build_usdt_universe()
    log.info("%d symbols passed basic universe filters", len(universe))
    results, errors = [], []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(screen_symbol, item, btc["return_6h"]): item["symbol"]
            for item in universe
        }
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as exc:
                errors.append((futures[future], str(exc)))
    if errors:
        log.warning("%d symbols had API/data errors; first examples: %s",
                    len(errors), errors[:5])
    if not results:
        return pd.DataFrame()
    ranking = pd.DataFrame(results).sort_values(
        ["eligible", "score", "relative_strength_vs_btc_pct", "volume_ratio_15m"],
        ascending=[False, False, False, False],
    )
    stamp = pd.Timestamp.now(tz="UTC").strftime("%Y%m%d_%H%M%S")
    ranking.to_csv(f"pre_pump_ranking_{stamp}.csv", index=False)
    selected = ranking[ranking["eligible"]].head(TOP_N).copy()
    return selected


def fmt_price(value):
    value = float(value)
    if value >= 1000:
        return f"{value:,.2f}"
    if value >= 1:
        return f"{value:.4f}".rstrip("0").rstrip(".")
    if value >= 0.01:
        return f"{value:.5f}".rstrip("0").rstrip(".")
    return f"{value:.8f}".rstrip("0").rstrip(".")


def make_telegram_message(selected):
    now = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d %H:%M UTC")
    lines = [f"💎 BINANCE GEMS | PRE-PUMP RADAR",
             f"🕒 Scan: {now}",
             f"📊 Qualified candidates: {len(selected)}"]
    if selected.empty:
        lines += ["", "No coin passed all strict filters this scan.",
                  "No trade is being recommended."]
    else:
        for i, (_, row) in enumerate(selected.iterrows(), start=1):
            lines += [
                "",
                f"{i}. {row['base_asset']} ( {row['symbol']} ) — Score {row['score']}/100",
                f"💵 Price: {fmt_price(row['close'])} USDT",
                f"📈 24h: {row['price_change_24h_pct']:+.2f}% | Vol: ${row['quote_volume_24h']/1e6:.1f}M",
                f"⚡ 15m volume: {row['volume_ratio_15m']:.2f}x | RSI: {row['rsi_15m']:.1f}",
                f"🔎 {row['reasons'] or 'Momentum conditions detected'}",
                "⚠️ Spot watch only — confirm your own entry and risk.",
            ]
    return "\n".join(lines)


def make_square_posts(selected):
    """Return concise, conversational 4–5-line Square drafts, one per coin."""
    posts = []
    for i, (_, row) in enumerate(selected.iterrows(), start=1):
        symbol = row["base_asset"]
        change = float(row["price_change_24h_pct"])
        vol_ratio = float(row["volume_ratio_15m"])
        score = int(row["score"])
        # Four short lines; avoids promises, target-price claims, or hype guarantees.
        post = (
            f"💎 BINANCE GEMS #{i} | ${symbol} is on my spot watchlist.\n"
            f"Score: {score}/100 • Price: {fmt_price(row['close'])} USDT\n"
            f"24h: {change:+.2f}% • 15m volume is {vol_ratio:.2f}× its average.\n"
            f"Momentum is building, but I’m watching confirmation—not chasing a candle."
        )
        posts.append((row["symbol"], post))
    return posts


def post_telegram(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram secrets missing; skipping Telegram post.")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    response = requests.post(
        url,
        json={"chat_id": TELEGRAM_CHAT_ID, "text": message,
              "disable_web_page_preview": True},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    payload = response.json()
    if not payload.get("ok"):
        raise RuntimeError(f"Telegram API returned not-ok: {payload}")
    log.info("Telegram post sent.")
    return True


def post_square_or_save_drafts(posts):
    """
    Automatic Square posting needs a supported, authorized endpoint.
    Generic endpoint contract used here: POST JSON {"text": "..."} with optional Bearer token.
    Configure only if you own/use a service that explicitly supports this contract.
    """
    if not posts:
        log.info("No qualified coins; no Square drafts.")
        return
    if SQUARE_API_URL:
        for symbol, content in posts:
            headers = {"Content-Type": "application/json"}
            if SQUARE_API_TOKEN:
                headers["Authorization"] = f"Bearer {SQUARE_API_TOKEN}"
            response = requests.post(
                SQUARE_API_URL,
                json={"text": content, "symbol": symbol},
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            log.info("Square endpoint accepted post for %s (HTTP %s)",
                     symbol, response.status_code)
        return

    draft_path = Path("square_drafts.txt")
    with draft_path.open("w", encoding="utf-8") as file:
        for symbol, content in posts:
            file.write(f"--- {symbol} ---\n{content}\n\n")
    log.warning(
        "No authorized Square API endpoint configured. Saved %d short post(s) "
        "to %s for manual posting.", len(posts), draft_path
    )


def main():
    selected = scan()
    telegram_message = make_telegram_message(selected)
    square_posts = make_square_posts(selected)
    # Telegram gets one combined message. Square gets one short post per candidate.
    post_telegram(telegram_message)
    post_square_or_save_drafts(square_posts)

    if not selected.empty:
        print("\nTOP SELECTED COINS")
        print(selected[["symbol", "score", "close", "volume_ratio_15m",
                        "price_change_24h_pct"]].to_string(index=False))
    else:
        print("No eligible coins this scan.")


if __name__ == "__main__":
    try:
        main()
    except requests.RequestException as exc:
        log.exception("Network/API error: %s", exc)
        raise
    except Exception as exc:
        log.exception("Unexpected error: %s", exc)
        raise
