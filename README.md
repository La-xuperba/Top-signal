# BINANCE GEMS — Early Pump Radar

Binance **Spot USDT only** signal scanner. It does not place orders and does not need Binance trading keys.

## Features
- All eligible Binance Spot USDT trading pairs (stablecoins and leveraged tokens excluded).
- 15m trigger + 1h setup + 4h trend.
- 100-point scoring; 65+ watchlist, 80+ strong candidate (hard filters still apply).
- CoinGecko market-cap lookup; optional CoinMarketCap fallback.
- Market-cap-relative 24h turnover thresholds: <$1M 15%, <$10M 8%, <$100M 5%, <$500M 3%, otherwise 2%.
- Risk sizing example: 0.75% account risk, max 25% reference position allocation.
- Telegram alerts with 24-hour per-symbol cooldown; no follow-up TP/SL result notifications.
- Mobile-friendly GitHub Pages dashboard.

## Install
1. Upload/extract the project files into your GitHub repository (or copy these files into the existing `binance-gems` repository).
2. Ensure the workflow is at `.github/workflows/binance-gems.yml`.
3. In GitHub → Settings → Secrets and variables → Actions, add:
   - `TELEGRAM_BOT_TOKEN`
   - `TELEGRAM_CHAT_ID`
   - Optional: `COINMARKETCAP_API_KEY` (CoinGecko is primary).
4. GitHub → Settings → Pages → Build and deployment → Source: **GitHub Actions**.
5. Actions → `BINANCE GEMS Early Pump Radar` → Run workflow.

The dashboard URL will be:
`https://YOUR_GITHUB_USERNAME.github.io/YOUR_REPOSITORY/`

## Notes
- The included workflow runs every 15 minutes to reduce public API rate-limit risk while requesting 15m/1h/4h candles for the eligible universe. GitHub schedule times are UTC and can be delayed by GitHub.
- All eligible symbols are enumerated; if Binance throttles a scan, affected symbols are skipped for that run and logged. Check Actions logs.
- CoinGecko's public API may rate-limit. Missing market cap is shown as N/A and receives no turnover score; no fabricated cap is used.
- `REFERENCE_CAPITAL_USD=1000` is only a sizing example. Change it in the workflow to your account's planning capital; no funds are accessed.
- Stop-loss and targets are analytical estimates, not exchange orders. Gaps, fees, slippage, and stop-limit non-fills can change actual results.
- This is a candidate scanner, not a promise of pre-pump detection or profit. Backtest and paper-trade before relying on it.

## Dashboard
`docs/index.html` reads `docs/data.json`. GitHub Pages is deployed after each successful workflow run.
