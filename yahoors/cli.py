import argparse
import datetime as dt
import json
import sys

from .modules.candles import Candles
from .modules.options import Options
from .modules.screener import options_screener
from .modules.statements import Statements
from .modules.tickers import Tickers
from .periphery.config import get_db_path
from .periphery.utils import clean_tickers
from .periphery.technical_analysis import add_indicators


def get_risk_free_rate(candles: Candles) -> float:
    prices = candles.get_last_price(["^TNX"])
    return prices.get("^TNX", 0.05) / 100.0


def cmd_earnings(args):
    # TODO: implement earnings
    print(f"Earnings for {args.ticker} not yet implemented")


def cmd_get_candles(args):
    db_path = str(get_db_path())
    candles = Candles(db_path, debug=False)
    tickers = clean_tickers(args.symbols)

    df = candles.get_candles(
        tickers,
        interval=args.interval,
        period=args.range,
    )

    if args.indicators:
        df = add_indicators(df)

    print(df)


def cmd_options(args):
    db_path = str(get_db_path())
    options = Options(db_path)
    tickers = clean_tickers(args.symbols)

    df = options.get_options(tickers, get_latest=True)
    print(df)


def cmd_options_screener(args):
    db_path = str(get_db_path())
    options = Options(db_path)
    tickers = clean_tickers(args.symbols)

    options_df = options.get_options(tickers, get_latest=True)

    results = options_screener(
        options_df,
        min_dte=args.min_dte,
        max_dte=args.max_dte,
        in_the_money=args.in_the_money,
        long=args.long,
        min_collateral=args.min_collateral,
        max_collateral=args.max_collateral,
    )

    print(
        results.select(
            [
                "contract_symbol",
                "side",
                "strike",
                "stock_price",
                "bid",
                "ask",
                "dte",
                "bs_price",
                "prob_profit",
                "hist_prob_profit",
                "expected_return",
            ]
        )
    )


def cmd_statements(args):
    db_path = str(get_db_path())
    statements = Statements(db_path)
    candles = Candles(db_path, debug=False)
    tickers = clean_tickers(args.symbols)
    period = "A" if args.annual else "Q"

    type_map = {
        "income": "income_statement",
        "balance": "balance_sheet",
        "cash": "cash_flow",
    }
    statement_type = type_map.get(args.statement_type, args.statement_type)

    df = statements.get_statement(tickers, statement_type, period)
    pivoted = statements.pivot_statement(df, period)

    if args.ratios:
        all_types = ["income_statement", "balance_sheet", "cash_flow"]
        remaining = [t for t in all_types if t != statement_type]

        for r in remaining:
            extra = statements.get_statement(tickers, r, period)
            extra_pivoted = statements.pivot_statement(extra, period)
            pivoted = pivoted.vstack(extra_pivoted)

        candles_df = candles.get_candles(tickers)
        ratios = statements.get_ratios(
            tickers,
            income_df=pivoted.filter(lambda: statement_type == "income_statement"),
            balance_sheet_df=pivoted,
            candles_df=candles_df,
            period=period,
        )
        print(ratios)

    elif args.margins:
        margins = statements.get_margins(pivoted)
        print(margins)

    else:
        print(pivoted)


def cmd_info(args):
    db_path = str(get_db_path())
    tickers_mod = Tickers(db_path)
    info = tickers_mod.get_info([args.ticker])
    print(info)


def main():
    parser = argparse.ArgumentParser(description="YahooRS - Yahoo Finance CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # earnings
    p_earnings = subparsers.add_parser("earnings")
    p_earnings.add_argument("-t", "--ticker", required=True)
    p_earnings.set_defaults(func=cmd_earnings)

    # get-candles
    p_candles = subparsers.add_parser("get-candles")
    p_candles.add_argument("symbols", nargs="+")
    p_candles.add_argument("-i", "--interval", default="1d")
    p_candles.add_argument("-r", "--range", default="max")
    p_candles.add_argument("--indicators", action="store_true")
    p_candles.add_argument("--force-update", action="store_true")
    p_candles.set_defaults(func=cmd_get_candles)

    # options
    p_options = subparsers.add_parser("options")
    p_options.add_argument("symbols", nargs="+")
    p_options.set_defaults(func=cmd_options)

    # options-screener
    p_screener = subparsers.add_parser("options-screener")
    p_screener.add_argument("-s", "--symbols", nargs="+", required=True)
    p_screener.add_argument("--min-dte", type=int, default=0)
    p_screener.add_argument("--max-dte", type=int, default=7)
    p_screener.add_argument("--in-the-money", action="store_true")
    p_screener.add_argument("--long", action="store_true")
    p_screener.add_argument("--min-collateral", type=float, default=0.0)
    p_screener.add_argument("--max-collateral", type=float, default=1000000.0)
    p_screener.add_argument("--force-update", action="store_true")
    p_screener.add_argument("--update-candles", action="store_true")
    p_screener.set_defaults(func=cmd_options_screener)

    # statements
    p_statements = subparsers.add_parser("statements")
    p_statements.add_argument("symbols", nargs="+")
    p_statements.add_argument("-s", "--statement-type", default="income")
    p_statements.add_argument("-a", "--annual", action="store_true")
    p_statements.add_argument("-q", "--quarterly", action="store_true")
    p_statements.add_argument("-m", "--margins", action="store_true")
    p_statements.add_argument("-r", "--ratios", action="store_true")
    p_statements.set_defaults(func=cmd_statements)

    # info
    p_info = subparsers.add_parser("info")
    p_info.add_argument("-t", "--ticker", required=True)
    p_info.set_defaults(func=cmd_info)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
