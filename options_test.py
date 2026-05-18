import polars as pl
from yahoors import Options
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

df.write_csv("csp.csv")
print(df)
