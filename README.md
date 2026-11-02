# BINANCE GEMS — Spot Early-Momentum Radar

A Python + GitHub Actions scanner for Binance Spot USDT pairs. It sends research alerts to Telegram only. **It does not place trades or manage exchange accounts.**

## What this starter version does
- Scans tradable Binance Spot USDT pairs.
- Uses closed 1H and 15M candles.
- Scores 1H EMA20/EMA50 trend, RSI, 15M volume expansion, EMA20 proximity, candle body/wick and taker-buy ratio.
- Avoids very extended 24H moves and nearby recent swing-high resistance.
- Sends entry/TP/risk reference levels to Telegram.
- Prevents repeat alerts for the same symbol on the same UTC day.
- Runs every 15 minutes in GitHub Actions; manual run is available.

## Important scope notes
This is a working first implementation of the core early-momentum scanner, not every advanced item in the historical wish list. Fibonacci zones, full order-book/order-flow analysis, FVG/AMD classification, supply-demand mapping, market-cap-aware turnover thresholds, CoinGecko/CMC enrichment, and Futures funding/OI/liquidation enrichment are not implemented in this starter. Do not assume those data points are included in a signal. The current score is technical/market-data based and is deliberately transparent.

The score maximum in this version is 58. The default minimum is 45. Signals are informational, not financial advice; TP and risk levels are ATR-based reference levels, not guaranteed outcomes.

## GitHub setup

1. Create a new **private** GitHub repository, for example `binance-gems`.
2. Download and unzip this project, then upload all files/folders to the repository root. Keep `.github/workflows/binance-gems.yml` at exactly that path.
3. Open the repository → **Settings** → **Secrets and variables** → **Actions** → **New repository secret**.
4. Add:
   - `TELEGRAM_BOT_TOKEN` — token from BotFather.
   - `TELEGRAM_CHAT_ID` — your Telegram user/group/channel chat ID. Add the bot to the channel/group and grant permission to post.
5. Open **Settings → Actions → General → Workflow permissions** and select **Read and write permissions** (needed to save duplicate-prevention state).
6. Open **Actions** → **BINANCE GEMS Spot Radar** → **Run workflow** to test manually.
7. Check the run log. If successful, it will run every 15 minutes automatically. GitHub scheduled workflows can be delayed and are not guaranteed to run exactly on the minute.

## Telegram setup
1. In Telegram, open `@BotFather`, create a bot with `/newbot`, and copy its token.
2. Add the bot to your private channel/group. For a channel, make it an administrator with permission to post messages.
3. Obtain the chat ID (for a channel it is commonly `@channelusername`; for private groups it is a numeric ID). Save it as the GitHub secret `TELEGRAM_CHAT_ID`.
4. Never paste the bot token into source code or commit it to GitHub.

## Local test
```bash
python -m venv .venv
# Windows: .venv\\Scripts\\activate
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
export TELEGRAM_BOT_TOKEN="..."   # optional for print-only test
export TELEGRAM_CHAT_ID="..."
python binance_bot.py --watch 12
```

To manually bypass same-day duplicate protection:
```bash
python binance_bot.py --watch 12 --force
```

## Tuning
- `MIN_SCORE` default `45`
- `MAX_24H_CHANGE_PCT` default `15`
- `STATE_FILE` default `state_binance.json`
- `BINANCE_BASE_URL` default `https://data-api.binance.vision`

## Troubleshooting
- **No signals:** filters may not be met in current market conditions. Review the Actions log; do not lower thresholds blindly.
- **Telegram not posting:** verify both secrets, bot membership, channel admin rights, and chat ID.
- **Workflow cannot push state:** enable Actions read/write permissions in repository settings.
- **API errors:** rerun later; public endpoints can rate-limit or temporarily fail.
