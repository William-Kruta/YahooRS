# YahooRS

YahooRS is a Python-based utility for fetching and managing Yahoo Finance data, leveraging DuckDB for efficient local storage and Polars for high-performance data manipulation. It provides tools for candle data, option chains (with Greeks), earnings data, and financial statements.

## Features

- **Historical Price Data (Candles):** Fetch and store historical price data with configurable intervals and periods. Includes automated staleness detection and local caching via `collected_at` timestamps — data is only re-downloaded when genuinely stale, not on every call.
- **Options Analysis:** Download full option chains with real-time Greeks (Delta, Gamma, Theta, Vega), Black-Scholes pricing, and probability of profit calculations (both BS-derived and historical). Supports filtering by DTE range, bid/ask minimums, option type, and long/short side.
- **Options Screener:** Ready-to-use strategies including `cash_secured_puts` and a general `options_screener` with yield metrics (premium, ROC, annualized ROC, collateral, expected return).
- **Earnings Data:** Earnings dates, EPS estimates, and history with per-dataset staleness handling and explicit force refreshes. Gracefully handles tickers with no earnings data (ETFs, etc.).
- **Financial Statements:** Retrieve annual and quarterly income statements, balance sheets, and cash flow statements.
- **Financial Ratios & Margins:** Automated calculation of key financial metrics such as P/E, P/S, P/B, EV/EBITDA, ROE, and various profit margins.
- **Local Database (DuckDB):** Persists all fetched data locally to minimize redundant API calls and enable fast offline analysis.
- **CLI & Library:** Accessible via a command-line interface or directly as a Python library.

## Installation

```bash
pip install yahoors
```

## Data Storage

By default, YahooRS stores data in a DuckDB database located in your platform's standard configuration directory (e.g., `~/.config/yahoors/` on Linux). You can override this by setting the `YAHOO_FINANCE_DB` environment variable.

## CLI Usage

The package installs a `yahoors` command with several subcommands:

### Fetch Candle Data
```bash
yahoors get-candles AAPL MSFT --interval 1d --range 1y

# Bypass the cache and refresh the requested range
yahoors get-candles AAPL MSFT --interval 1d --range 1y --force-update
```

### Options Screener
```bash
yahoors options-screener -s AAPL --min-dte 30 --max-dte 60
```

### Financial Statements
```bash
yahoors statements AAPL --statement-type income --annual --ratios

# Refresh statements and their candle inputs before calculating ratios
yahoors statements AAPL --statement-type income --annual --ratios --force-update
```

## Library Usage

### Candle Data

```python
from yahoors import Candles

candles = Candles()

# Fetch historical data (cached — only downloads when stale)
df = candles.get_candles(["AAPL", "MSFT"], interval="1d")

# Bypass staleness checks and replace cached OHLCV values
fresh_df = candles.get_candles(
    ["AAPL", "MSFT"],
    interval="1d",
    period="1y",
    force_update=True,
)

# Get the latest closing price without loading full history
prices = candles.get_last_price(["AAPL", "MSFT"])
# {"AAPL": 189.30, "MSFT": 415.20}
```

### Options

```python
from yahoors import Options

options = Options()

# Full option chain with Greeks and probability metrics
df = options.get_options(["AAPL"])

# Filter by DTE range with side-aware probability of profit
df = options.get_options_by_dte_range(
    ["AAPL", "MSFT"],
    min_dte=1,
    max_dte=10,
    option_type="put",   # "call", "put", or "*"
    side="short",        # "long", "short", or "*" — inverts prob_profit for short positions
    min_bid=0.10,        # filter illiquid contracts
)
```

### Options Screener

```python
from yahoors.modules.screener import cash_secured_puts, options_screener

# Ready-to-use cash-secured put screener
# Returns contracts sorted by expected_return, with yield metrics pre-calculated
df = cash_secured_puts(
    ["AAPL", "MSFT", "AMZN"],
    min_dte=1,
    max_dte=10,
    max_collateral=25_000,   # max capital at risk per contract (strike * 100)
    min_premium=0.10,
    min_roc=0.005,
)
# Columns include: strike, premium, collateral, roc, annualized_roc,
#                  prob_profit, hist_prob_profit, expected_return, dtr, ...

# General screener — pass any options DataFrame. Short calls are modeled as
# covered calls for collateral and maximum-loss calculations.
df = options_screener(
    options_df,
    min_dte=0,
    max_dte=30,
    long=False,
    min_collateral=0,
    max_collateral=50_000,
    min_premium=0.10,
    min_roc=0.005,
    max_trade_age=dt.timedelta(hours=2),
)
```

### Earnings

```python
from yahoors import Earnings

earnings = Earnings()

# Upcoming and historical earnings dates
dates_df = earnings.get_earnings_dates(["AAPL", "MSFT"])

# Every earnings getter also supports an explicit refresh
fresh_dates_df = earnings.get_earnings_dates(["AAPL"], force_update=True)

# EPS estimates
estimates_df = earnings.get_earnings_estimates(["AAPL"])

# Historical EPS actuals vs estimates
history_df = earnings.get_earnings_history(["AAPL"])
```

### Financial Statements

```python
from yahoors import Statements

with Statements() as statements:
    df = statements.get_statement(
        ["AAPL"],
        statement="income_statement",
        period="A",
        force_update=True,
    )
```

Statement column names preserve Yahoo's actual fiscal period-end dates. Missing
values remain null rather than being converted to zero. Successful statement
downloads use the normal annual or quarterly cache lifetime; empty responses use
a one-day negative cache so temporary upstream gaps recover promptly.

## Resource Management

DuckDB-backed classes implement context managers. Use them for short-lived jobs
so every database connection is closed deterministically:

```python
from yahoors import Candles, Options, Statements

with Candles() as candles:
    prices = candles.get_last_price(["AAPL", "MSFT"])

with Options() as options:
    chain = options.get_options(["AAPL"])

with Statements() as statements:
    income = statements.get_income_statement("AAPL", period="A")
```

Long-lived instances can instead call `close()` explicitly. The HTTP server
serializes access to its shared DuckDB-backed modules and closes them during
application shutdown.

## Tests

The normal suite is offline:

```bash
pytest -q
```

Two optional Yahoo Finance smoke tests validate live candle and statement
responses:

```bash
YAHOORS_RUN_LIVE_TESTS=1 pytest -q -m live
```

## Probability of Profit

YahooRS computes two probability metrics for each contract:

- **`prob_profit`** — Black-Scholes derived, using the contract's implied volatility and breakeven price.
- **`hist_prob_profit`** — Historical, derived from the actual distribution of past returns over the contract's DTE window.

For `side="short"`, both are automatically inverted (`1 - p`) so they represent the seller's probability of profit. Contracts where IV cannot be computed are excluded from cached option snapshots and screener results. Historical return windows use calendar DTE rather than treating DTE as a count of trading sessions.

## Expected Return

The `expected_return` column in screener output is computed as:

```
expected_return = (premium - bs_price) / strike
```

For short contracts, this represents premium collected above Black-Scholes fair value, normalized by strike. Long contracts reverse the calculation to `(bs_price - premium) / strike`, so positive values consistently represent a favorable model edge. Zero-DTE contracts retain non-annualized ROC but return `null` for `annualized_roc`.

Dividend yield uses the sum of all payments in the trailing 365 calendar days, so monthly, quarterly, annual, and irregular payment schedules are handled consistently.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
