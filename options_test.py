import argparse

import polars as pl
from yahoors.modules.screener import cash_secured_puts


watchlist = [
    "RKLB",
    "ASTS",
    "PL",
    "RDW",
    "SOFI",
    "RIVN",
    "POET",
    "LUNR",
    "DRAM",
    "MRNA",
    "NVO",
    "SMR",
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the example CSP screen.")
    parser.add_argument(
        "--output",
        help="Optional CSV output path. Results are only printed by default.",
    )
    args = parser.parse_args()

    df = cash_secured_puts(watchlist, max_dte=10, max_collateral=50_000)
    df = df.filter(pl.col("expected_return") > 0)
    select = [
        "contract_symbol",
        "strike",
        "stock_price",
        "bid",
        "ask",
        "prob_profit",
        "annualized_roc",
        "dte",
        "dtr",
    ]
    df = df.select(select)

    if args.output:
        df.write_csv(args.output)
    print(df)


if __name__ == "__main__":
    main()
