import datetime as dt
from dataclasses import asdict, dataclass
from typing import Any, Literal

from fastapi import FastAPI, Query
import uvicorn

from .modules.candles import Candles
from .modules.dividends import Dividends
from .modules.earnings import Earnings
from .modules.macro import Macro
from .modules.options import Options
from .modules.screener import options_screener
from .modules.statements import Statements
from .modules.tickers import Ticker


StatementType = Literal["income_statement", "balance_sheet", "cash_flow"]
EarningsType = Literal["dates", "estimates", "history"]


@dataclass
class DataFrameResponse:
    rows: list[dict[str, Any]]
    row_count: int


def _serialize_value(value: Any) -> Any:
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _serialize_value(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_serialize_value(item) for item in value]
    return value


def _frame_response(df: Any) -> dict[str, Any]:
    rows = [_serialize_value(row) for row in df.to_dicts()]
    return asdict(DataFrameResponse(rows=rows, row_count=len(rows)))


def _normalize_tickers(tickers: list[str] | str) -> list[str]:
    if isinstance(tickers, str):
        return [tickers]
    return tickers


class YahooRSAPI:
    def __init__(self, db_path: str | None = None):
        self.db_path = db_path
        self.candles = Candles(db_path=db_path, debug=False)
        self.options = Options(db_path=db_path)
        self.statements = Statements(db_path=db_path, candles_obj=self.candles)
        self.earnings = Earnings(db_path=db_path)
        self.dividends = Dividends(db_path=db_path, debug=False)
        self.macro = Macro(db_path=db_path, candles_obj=self.candles)

    def get_candles(
        self,
        tickers: list[str] | str,
        interval: str = "1d",
        period: str = "max",
    ) -> dict[str, Any]:
        df = self.candles.get_candles(
            tickers=_normalize_tickers(tickers),
            interval=interval,
            period=period,
        )
        return _frame_response(df)

    def get_last_price(
        self,
        tickers: list[str] | str,
        select_col: str = "close",
        alias: str = "value",
    ) -> dict[str, Any]:
        values = self.candles.get_last_price(
            tickers=_normalize_tickers(tickers),
            select_col=select_col,
            alias=alias,
        )
        return {"data": _serialize_value(values), "count": len(values)}

    def get_options(
        self,
        tickers: list[str] | str,
        get_latest: bool = True,
        expirations: list[str] | None = None,
        force_update: bool = False,
    ) -> dict[str, Any]:
        df = self.options.get_options(
            tickers=_normalize_tickers(tickers),
            get_latest=get_latest,
            expirations=expirations or [],
            force_update=force_update,
        )
        return _frame_response(df)

    def screen_options(
        self,
        tickers: list[str] | str,
        min_dte: int = 0,
        max_dte: int = 365,
        in_the_money: bool = False,
        long: bool = False,
        min_collateral: float = 0.0,
        max_collateral: float = float("inf"),
        force_update: bool = False,
    ) -> dict[str, Any]:
        options_df = self.options.get_options(
            tickers=_normalize_tickers(tickers),
            get_latest=True,
            force_update=force_update,
        )
        screened = options_screener(
            options_df=options_df,
            min_dte=min_dte,
            max_dte=max_dte,
            in_the_money=in_the_money,
            long=long,
            min_collateral=min_collateral,
            max_collateral=max_collateral,
        )
        return _frame_response(screened)

    def get_statement(
        self,
        tickers: list[str] | str,
        statement_type: StatementType,
        period: Literal["A", "Q"] = "A",
    ) -> dict[str, Any]:
        df = self.statements.get_statement(
            tickers=_normalize_tickers(tickers),
            statement=statement_type,
            period=period,
        )
        return _frame_response(df)

    def get_margins(
        self,
        tickers: list[str] | str,
        period: Literal["A", "Q"] = "A",
    ) -> dict[str, Any]:
        df = self.statements.get_margins(
            tickers=_normalize_tickers(tickers),
            period=period,
        )
        return _frame_response(df)

    def get_ratios(
        self,
        tickers: list[str] | str,
        period: Literal["A", "Q"] = "A",
    ) -> dict[str, Any]:
        normalized = _normalize_tickers(tickers)
        income_df = self.statements.get_statement(normalized, "income_statement", period)
        balance_df = self.statements.get_statement(normalized, "balance_sheet", period)
        candles_df = self.candles.get_candles(normalized)
        df = self.statements.get_ratios(
            tickers=normalized,
            income_df=income_df,
            balance_sheet_df=balance_df,
            candles_df=candles_df,
            period=period,
        )
        return _frame_response(df)

    def get_earnings(
        self,
        tickers: list[str] | str,
        earnings_type: EarningsType,
    ) -> dict[str, Any]:
        normalized = _normalize_tickers(tickers)
        method_map = {
            "dates": self.earnings.get_earnings_dates,
            "estimates": self.earnings.get_earnings_estimates,
            "history": self.earnings.get_earnings_history,
        }
        df = method_map[earnings_type](normalized)
        return _frame_response(df)

    def get_dividends(self, tickers: list[str] | str) -> dict[str, Any]:
        df = self.dividends.get_dividends(_normalize_tickers(tickers))
        return _frame_response(df)

    def get_risk_free_rate(
        self,
        ticker: str = "^TNX",
        interval: str = "1d",
        period: str = "max",
    ) -> dict[str, Any]:
        df = self.macro.get_risk_free_rate(
            ticker=ticker,
            interval=interval,
            period=period,
        )
        return _frame_response(df)

    def get_yield_curve(
        self,
        short_term_ticker: str = "2YY=F",
        long_term_ticker: str = "^TNX",
        interval: str = "1d",
        period: str = "max",
    ) -> dict[str, Any]:
        df = self.macro.get_yield_curve(
            short_term_ticker=short_term_ticker,
            long_term_ticker=long_term_ticker,
            interval=interval,
            period=period,
        )
        return _frame_response(df)

    def get_currency_exchange_rate(
        self,
        currency_a: str,
        currency_b: str,
        interval: str = "1d",
        period: str = "max",
    ) -> dict[str, Any]:
        df = self.macro.get_currency_exchange_rate(
            currency_a=currency_a,
            currency_b=currency_b,
            interval=interval,
            period=period,
        )
        return _frame_response(df)

    def get_ticker_info(self, ticker: str) -> dict[str, Any]:
        df = Ticker(ticker).info
        return _frame_response(df)

    def get_ticker_trading_status(self, ticker: str) -> dict[str, Any]:
        df = Ticker(ticker).trading_status
        return _frame_response(df)


def create_app(db_path: str | None = None) -> FastAPI:
    app = FastAPI(
        title="YahooRS API",
        version="0.1.4",
        description="HTTP API for YahooRS market data modules.",
    )

    def get_api() -> YahooRSAPI:
        if not hasattr(app.state, "api"):
            app.state.api = YahooRSAPI(db_path=db_path)
        return app.state.api

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/candles")
    def get_candles(
        tickers: list[str] = Query(...),
        interval: str = "1d",
        period: str = "max",
    ) -> dict[str, Any]:
        return get_api().get_candles(tickers=tickers, interval=interval, period=period)

    @app.get("/candles/last-price")
    def get_last_price(
        tickers: list[str] = Query(...),
        select_col: str = "close",
        alias: str = "value",
    ) -> dict[str, Any]:
        return get_api().get_last_price(
            tickers=tickers,
            select_col=select_col,
            alias=alias,
        )

    @app.get("/options")
    def get_options(
        tickers: list[str] = Query(...),
        get_latest: bool = True,
        expirations: list[str] | None = Query(default=None),
        force_update: bool = False,
    ) -> dict[str, Any]:
        return get_api().get_options(
            tickers=tickers,
            get_latest=get_latest,
            expirations=expirations,
            force_update=force_update,
        )

    @app.get("/options/screener")
    def get_option_screener(
        tickers: list[str] = Query(...),
        min_dte: int = 0,
        max_dte: int = 365,
        in_the_money: bool = False,
        long: bool = False,
        min_collateral: float = 0.0,
        max_collateral: float = float("inf"),
        force_update: bool = False,
    ) -> dict[str, Any]:
        return get_api().screen_options(
            tickers=tickers,
            min_dte=min_dte,
            max_dte=max_dte,
            in_the_money=in_the_money,
            long=long,
            min_collateral=min_collateral,
            max_collateral=max_collateral,
            force_update=force_update,
        )

    @app.get("/statements/margins")
    def get_margins(
        tickers: list[str] = Query(...),
        period: Literal["A", "Q"] = "A",
    ) -> dict[str, Any]:
        return get_api().get_margins(tickers=tickers, period=period)

    @app.get("/statements/ratios")
    def get_ratios(
        tickers: list[str] = Query(...),
        period: Literal["A", "Q"] = "A",
    ) -> dict[str, Any]:
        return get_api().get_ratios(tickers=tickers, period=period)

    @app.get("/statements/{statement_type}")
    def get_statement(
        statement_type: StatementType,
        tickers: list[str] = Query(...),
        period: Literal["A", "Q"] = "A",
    ) -> dict[str, Any]:
        return get_api().get_statement(
            tickers=tickers,
            statement_type=statement_type,
            period=period,
        )

    @app.get("/earnings/{earnings_type}")
    def get_earnings(
        earnings_type: EarningsType,
        tickers: list[str] = Query(...),
    ) -> dict[str, Any]:
        return get_api().get_earnings(tickers=tickers, earnings_type=earnings_type)

    @app.get("/dividends")
    def get_dividends(tickers: list[str] = Query(...)) -> dict[str, Any]:
        return get_api().get_dividends(tickers=tickers)

    @app.get("/macro/risk-free-rate")
    def get_risk_free_rate(
        ticker: str = "^TNX",
        interval: str = "1d",
        period: str = "max",
    ) -> dict[str, Any]:
        return get_api().get_risk_free_rate(
            ticker=ticker,
            interval=interval,
            period=period,
        )

    @app.get("/macro/yield-curve")
    def get_yield_curve(
        short_term_ticker: str = "2YY=F",
        long_term_ticker: str = "^TNX",
        interval: str = "1d",
        period: str = "max",
    ) -> dict[str, Any]:
        return get_api().get_yield_curve(
            short_term_ticker=short_term_ticker,
            long_term_ticker=long_term_ticker,
            interval=interval,
            period=period,
        )

    @app.get("/macro/exchange-rate")
    def get_exchange_rate(
        currency_a: str,
        currency_b: str,
        interval: str = "1d",
        period: str = "max",
    ) -> dict[str, Any]:
        return get_api().get_currency_exchange_rate(
            currency_a=currency_a,
            currency_b=currency_b,
            interval=interval,
            period=period,
        )

    @app.get("/tickers/{ticker}/info")
    def get_ticker_info(ticker: str) -> dict[str, Any]:
        return get_api().get_ticker_info(ticker=ticker)

    @app.get("/tickers/{ticker}/trading-status")
    def get_ticker_trading_status(ticker: str) -> dict[str, Any]:
        return get_api().get_ticker_trading_status(ticker=ticker)

    return app


app = create_app()


def run(
    host: str = "127.0.0.1",
    port: int = 8000,
    db_path: str | None = None,
    reload: bool = False,
) -> None:
    if db_path is None:
        uvicorn.run("yahoors.server:app", host=host, port=port, reload=reload)
        return
    uvicorn.run(create_app(db_path=db_path), host=host, port=port, reload=reload)


if __name__ == "__main__":
    run()
