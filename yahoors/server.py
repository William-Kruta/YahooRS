import datetime as dt
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from functools import wraps
from threading import RLock
from typing import Any, Literal

from fastapi import FastAPI, Query
import uvicorn

from ._version import __version__
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
CandleValueColumn = Literal["date", "open", "high", "low", "close", "volume", "collected_at"]


@dataclass
class DataFrameResponse:
    rows: list[dict[str, Any]]
    row_count: int


def _serialized(method):
    """Serialize access to the API's shared DuckDB-backed module instances."""

    @wraps(method)
    def wrapper(self, *args, **kwargs):
        lock = getattr(self, "_lock", None)
        if lock is None:
            lock = RLock()
            self._lock = lock
        with lock:
            return method(self, *args, **kwargs)

    return wrapper


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
        self._lock = RLock()
        self.candles = Candles(db_path=db_path, debug=False)
        self.options = Options(db_path=db_path)
        self.statements = Statements(db_path=db_path, candles_obj=self.candles)
        self.earnings = Earnings(db_path=db_path)
        self.dividends = Dividends(db_path=db_path, debug=False)
        self.macro = Macro(db_path=db_path, candles_obj=self.candles)

    @_serialized
    def close(self) -> None:
        self.options.close()
        self.earnings.close()
        self.dividends.close()
        self.statements.close()
        self.macro.close()
        self.candles.close()

    def __enter__(self) -> "YahooRSAPI":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    @_serialized
    def get_candles(
        self,
        tickers: list[str] | str,
        interval: str = "1d",
        period: str = "max",
        force_update: bool = False,
    ) -> dict[str, Any]:
        df = self.candles.get_candles(
            tickers=_normalize_tickers(tickers),
            interval=interval,
            period=period,
            force_update=force_update,
        )
        return _frame_response(df)

    @_serialized
    def get_last_price(
        self,
        tickers: list[str] | str,
        select_col: CandleValueColumn = "close",
        alias: str = "value",
        force_update: bool = False,
    ) -> dict[str, Any]:
        values = self.candles.get_last_price(
            tickers=_normalize_tickers(tickers),
            select_col=select_col,
            alias=alias,
            force_update=force_update,
        )
        return {"data": _serialize_value(values), "count": len(values)}

    @_serialized
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

    @_serialized
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
        options_df = self.options.get_options_by_dte_range(
            tickers=_normalize_tickers(tickers),
            min_dte=min_dte,
            max_dte=max_dte,
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

    @_serialized
    def get_statement(
        self,
        tickers: list[str] | str,
        statement_type: StatementType,
        period: Literal["A", "Q"] = "A",
        force_update: bool = False,
    ) -> dict[str, Any]:
        df = self.statements.get_statement(
            tickers=_normalize_tickers(tickers),
            statement=statement_type,
            period=period,
            force_update=force_update,
        )
        return _frame_response(df)

    @_serialized
    def get_margins(
        self,
        tickers: list[str] | str,
        period: Literal["A", "Q"] = "A",
        force_update: bool = False,
    ) -> dict[str, Any]:
        df = self.statements.get_margins(
            tickers=_normalize_tickers(tickers),
            period=period,
            force_update=force_update,
        )
        return _frame_response(df)

    @_serialized
    def get_ratios(
        self,
        tickers: list[str] | str,
        period: Literal["A", "Q"] = "A",
        force_update: bool = False,
    ) -> dict[str, Any]:
        normalized = _normalize_tickers(tickers)
        income_df = self.statements.get_statement(
            normalized,
            "income_statement",
            period,
            force_update=force_update,
        )
        balance_df = self.statements.get_statement(
            normalized,
            "balance_sheet",
            period,
            force_update=force_update,
        )
        candles_df = self.candles.get_candles(normalized, force_update=force_update)
        df = self.statements.get_ratios(
            tickers=normalized,
            income_df=income_df,
            balance_sheet_df=balance_df,
            candles_df=candles_df,
            period=period,
        )
        return _frame_response(df)

    @_serialized
    def get_earnings(
        self,
        tickers: list[str] | str,
        earnings_type: EarningsType,
        force_update: bool = False,
    ) -> dict[str, Any]:
        normalized = _normalize_tickers(tickers)
        method_map = {
            "dates": self.earnings.get_earnings_dates,
            "estimates": self.earnings.get_earnings_estimates,
            "history": self.earnings.get_earnings_history,
        }
        df = method_map[earnings_type](normalized, force_update=force_update)
        return _frame_response(df)

    @_serialized
    def get_dividends(
        self,
        tickers: list[str] | str,
        force_update: bool = False,
    ) -> dict[str, Any]:
        df = self.dividends.get_dividends(
            _normalize_tickers(tickers),
            force_update=force_update,
        )
        return _frame_response(df)

    @_serialized
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

    @_serialized
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

    @_serialized
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

    @_serialized
    def get_ticker_info(self, ticker: str) -> dict[str, Any]:
        with Ticker(ticker, db_path=self.db_path) as ticker_obj:
            df = ticker_obj.info
        return _frame_response(df)

    @_serialized
    def get_ticker_trading_status(self, ticker: str) -> dict[str, Any]:
        with Ticker(ticker, db_path=self.db_path) as ticker_obj:
            df = ticker_obj.trading_status
        return _frame_response(df)


def create_app(db_path: str | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        api = getattr(app.state, "api", None)
        if api is not None:
            api.close()

    app = FastAPI(
        title="YahooRS API",
        version=__version__,
        description="HTTP API for YahooRS market data modules.",
        lifespan=lifespan,
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
        force_update: bool = False,
    ) -> dict[str, Any]:
        return get_api().get_candles(
            tickers=tickers,
            interval=interval,
            period=period,
            force_update=force_update,
        )

    @app.get("/candles/last-price")
    def get_last_price(
        tickers: list[str] = Query(...),
        select_col: CandleValueColumn = "close",
        alias: str = Query("value", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$"),
        force_update: bool = False,
    ) -> dict[str, Any]:
        return get_api().get_last_price(
            tickers=tickers,
            select_col=select_col,
            alias=alias,
            force_update=force_update,
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
        force_update: bool = False,
    ) -> dict[str, Any]:
        return get_api().get_margins(
            tickers=tickers,
            period=period,
            force_update=force_update,
        )

    @app.get("/statements/ratios")
    def get_ratios(
        tickers: list[str] = Query(...),
        period: Literal["A", "Q"] = "A",
        force_update: bool = False,
    ) -> dict[str, Any]:
        return get_api().get_ratios(
            tickers=tickers,
            period=period,
            force_update=force_update,
        )

    @app.get("/statements/{statement_type}")
    def get_statement(
        statement_type: StatementType,
        tickers: list[str] = Query(...),
        period: Literal["A", "Q"] = "A",
        force_update: bool = False,
    ) -> dict[str, Any]:
        return get_api().get_statement(
            tickers=tickers,
            statement_type=statement_type,
            period=period,
            force_update=force_update,
        )

    @app.get("/earnings/{earnings_type}")
    def get_earnings(
        earnings_type: EarningsType,
        tickers: list[str] = Query(...),
        force_update: bool = False,
    ) -> dict[str, Any]:
        return get_api().get_earnings(
            tickers=tickers,
            earnings_type=earnings_type,
            force_update=force_update,
        )

    @app.get("/dividends")
    def get_dividends(
        tickers: list[str] = Query(...),
        force_update: bool = False,
    ) -> dict[str, Any]:
        return get_api().get_dividends(
            tickers=tickers,
            force_update=force_update,
        )

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
