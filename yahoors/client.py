from typing import Any, Literal

import httpx
import polars as pl


StatementType = Literal["income_statement", "balance_sheet", "cash_flow"]
EarningsType = Literal["dates", "estimates", "history"]


class YahooRSClient:
    def __init__(
        self,
        base_url: str,
        timeout: float = 30.0,
        client: httpx.Client | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self._owns_client = client is None
        self._client = client or httpx.Client(
            base_url=self.base_url,
            timeout=timeout,
        )

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "YahooRSClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def health(self) -> dict[str, Any]:
        return self._get_json("/health")

    def get_candles(
        self,
        tickers: list[str] | str,
        interval: str = "1d",
        period: str = "max",
    ) -> pl.DataFrame:
        return self._get_frame(
            "/candles",
            params={
                "tickers": _normalize_tickers(tickers),
                "interval": interval,
                "period": period,
            },
        )

    def get_last_price(
        self,
        tickers: list[str] | str,
        select_col: str = "close",
        alias: str = "value",
    ) -> dict[str, Any]:
        payload = self._get_json(
            "/candles/last-price",
            params={
                "tickers": _normalize_tickers(tickers),
                "select_col": select_col,
                "alias": alias,
            },
        )
        return payload["data"]

    def get_options(
        self,
        tickers: list[str] | str,
        get_latest: bool = True,
        expirations: list[str] | None = None,
        force_update: bool = False,
    ) -> pl.DataFrame:
        params: dict[str, Any] = {
            "tickers": _normalize_tickers(tickers),
            "get_latest": get_latest,
            "force_update": force_update,
        }
        if expirations:
            params["expirations"] = expirations
        return self._get_frame("/options", params=params)

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
    ) -> pl.DataFrame:
        return self._get_frame(
            "/options/screener",
            params={
                "tickers": _normalize_tickers(tickers),
                "min_dte": min_dte,
                "max_dte": max_dte,
                "in_the_money": in_the_money,
                "long": long,
                "min_collateral": min_collateral,
                "max_collateral": max_collateral,
                "force_update": force_update,
            },
        )

    def get_statement(
        self,
        tickers: list[str] | str,
        statement_type: StatementType,
        period: Literal["A", "Q"] = "A",
    ) -> pl.DataFrame:
        return self._get_frame(
            f"/statements/{statement_type}",
            params={"tickers": _normalize_tickers(tickers), "period": period},
        )

    def get_margins(
        self,
        tickers: list[str] | str,
        period: Literal["A", "Q"] = "A",
    ) -> pl.DataFrame:
        return self._get_frame(
            "/statements/margins",
            params={"tickers": _normalize_tickers(tickers), "period": period},
        )

    def get_ratios(
        self,
        tickers: list[str] | str,
        period: Literal["A", "Q"] = "A",
    ) -> pl.DataFrame:
        return self._get_frame(
            "/statements/ratios",
            params={"tickers": _normalize_tickers(tickers), "period": period},
        )

    def get_earnings(
        self,
        tickers: list[str] | str,
        earnings_type: EarningsType,
    ) -> pl.DataFrame:
        return self._get_frame(
            f"/earnings/{earnings_type}",
            params={"tickers": _normalize_tickers(tickers)},
        )

    def get_dividends(self, tickers: list[str] | str) -> pl.DataFrame:
        return self._get_frame(
            "/dividends",
            params={"tickers": _normalize_tickers(tickers)},
        )

    def get_risk_free_rate(
        self,
        ticker: str = "^TNX",
        interval: str = "1d",
        period: str = "max",
    ) -> pl.DataFrame:
        return self._get_frame(
            "/macro/risk-free-rate",
            params={"ticker": ticker, "interval": interval, "period": period},
        )

    def get_yield_curve(
        self,
        short_term_ticker: str = "2YY=F",
        long_term_ticker: str = "^TNX",
        interval: str = "1d",
        period: str = "max",
    ) -> pl.DataFrame:
        return self._get_frame(
            "/macro/yield-curve",
            params={
                "short_term_ticker": short_term_ticker,
                "long_term_ticker": long_term_ticker,
                "interval": interval,
                "period": period,
            },
        )

    def get_currency_exchange_rate(
        self,
        currency_a: str,
        currency_b: str,
        interval: str = "1d",
        period: str = "max",
    ) -> pl.DataFrame:
        return self._get_frame(
            "/macro/exchange-rate",
            params={
                "currency_a": currency_a,
                "currency_b": currency_b,
                "interval": interval,
                "period": period,
            },
        )

    def get_ticker_info(self, ticker: str) -> pl.DataFrame:
        return self._get_frame(f"/tickers/{ticker}/info")

    def get_ticker_trading_status(self, ticker: str) -> pl.DataFrame:
        return self._get_frame(f"/tickers/{ticker}/trading-status")

    def _get_json(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        response = self._client.get(path, params=params)
        response.raise_for_status()
        return response.json()

    def _get_frame(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> pl.DataFrame:
        payload = self._get_json(path, params=params)
        return pl.DataFrame(payload.get("rows", []))


def _normalize_tickers(tickers: list[str] | str) -> list[str]:
    if isinstance(tickers, str):
        return [tickers]
    return tickers
