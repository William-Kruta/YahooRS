# YahooRS

YahooRS is a Python-based utility for fetching and managing Yahoo Finance data, leveraging DuckDB for efficient local storage and Polars for high-performance data manipulation. It provides tools for candle data, option chains (with Greeks), and financial statements.

## Features

- **Historical Price Data (Candles):** Fetch and store historical price data with configurable intervals and periods. Includes automated staleness detection and local caching.
- **Options Analysis:** Download full option chains, including real-time Greeks (Delta, Gamma, Theta, Vega), Black-Scholes pricing, and historical probability of profit calculations.
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
```

### Options Screener
```bash
yahoors options-screener -s AAPL --min-dte 30 --max-dte 60
```

### Financial Statements
```bash
yahoors statements AAPL --statement-type income --annual --ratios
```

## Library Usage

```python
from YahooRS.modules.candles import Candles
from YahooRS.modules.options import Options

# Initialize with default database
candles = Candles()
options = Options()

# Fetch data
df = candles.get_candles(["AAPL"])
option_chain = options.get_options(["AAPL"])
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
