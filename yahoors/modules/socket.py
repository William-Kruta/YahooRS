import csv
import datetime as dt
from pathlib import Path
from collections.abc import Callable, Iterator
from queue import Empty, Queue
from threading import Thread

import polars as pl
import yfinance as yf

from ..periphery.db import _init_tables, insert_data
from ..periphery.utils import clean_tickers


class WebSocket:
    def __init__(
        self,
        csv_path: str | None = None,
        db_path: str = None,
        url: str = "wss://streamer.finance.yahoo.com/?version=2",
        verbose: bool = True,
        persist: bool | None = None,
    ):
        if persist is None:
            persist = csv_path is not None
        if persist and csv_path is None:
            raise ValueError("csv_path is required when persist=True.")

        self.csv_path = Path(csv_path) if csv_path is not None else None
        self.conn = _init_tables(db_path)
        self.table_name = "candles"
        self.url = url
        self.verbose = verbose
        self.persist = persist
        self.ws = yf.WebSocket(url=url, verbose=verbose)
        self._tickers: list[str] = []
        self._interval: str | None = None
        self._ingested = False
        self._closed = False
        if self.persist:
            self._ensure_csv()

    def connect(self, tickers: list[str] | str, interval: str):
        if isinstance(tickers, str):
            tickers = [tickers]
        self._tickers = clean_tickers(tickers)
        self._interval = interval
        self._ingested = False
        self._closed = False
        self.ws.subscribe(self._tickers)
        return self

    def listen(self, on_row: Callable[[dict], None] | None = None):
        if not self._tickers:
            raise ValueError("Call connect() with at least one ticker before listen().")
        if self._interval is None:
            raise ValueError("Call connect() with an interval before listen().")

        caught_exception = None
        try:
            self.ws.listen(message_handler=lambda message: self._on_message(message, on_row))
        except KeyboardInterrupt:
            pass
        except Exception as exc:
            caught_exception = exc
        finally:
            self.close()

        if caught_exception is not None:
            raise caught_exception

    def stream(
        self,
        tickers: list[str] | str,
        interval: str,
        on_row: Callable[[dict], None] | None = None,
    ):
        self.connect(tickers, interval)
        self.listen(on_row=on_row)

    def iter_stream(self, tickers: list[str] | str, interval: str) -> Iterator[dict]:
        self.connect(tickers, interval)

        rows: Queue[dict | object] = Queue()
        sentinel = object()
        errors: list[Exception] = []

        def _run():
            try:
                self.listen(on_row=rows.put)
            except Exception as exc:
                errors.append(exc)
            finally:
                rows.put(sentinel)

        listener = Thread(target=_run, daemon=True)
        listener.start()

        try:
            while True:
                if self._closed:
                    break
                try:
                    item = rows.get(timeout=0.5)
                except Empty:
                    continue
                if item is sentinel:
                    break
                yield item
        finally:
            self.close()
            listener.join(timeout=1)

        if errors:
            raise errors[0]

    def close(self):
        try:
            self.ws.close()
        finally:
            self._ingest_csv_to_db()

    def _ensure_csv(self):
        if not self.persist or self.csv_path is None:
            return
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)
        if self.csv_path.exists() and self.csv_path.stat().st_size > 0:
            return
        with self.csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(
                ["date", "ticker", "open", "high", "low", "close", "volume"]
            )

    def _on_message(self, message: dict, on_row: Callable[[dict], None] | None = None):
        row = self._normalize_message(message)
        if row is None:
            return

        if on_row is not None:
            on_row(row)
        if self.persist:
            self._append_row(row)

    def _normalize_message(self, message: dict) -> dict | None:
        ticker = message.get("id")
        price = message.get("price")
        timestamp = message.get("time")

        if ticker is None or price is None or timestamp is None:
            return None

        return {
            "date": self._parse_timestamp(timestamp),
            "ticker": str(ticker),
            # Yahoo's live stream is tick/quote data, not candle data.
            # We persist the tick price in OHLC slots and rebuild candles later.
            "open": float(price),
            "high": float(price),
            "low": float(price),
            "close": float(price),
            # day_volume is cumulative for the trading session. The CSV reader
            # converts this to per-interval volume using diffs.
            "volume": int(message.get("day_volume", 0) or 0),
        }

    def _append_row(self, row: dict):
        if self.csv_path is None:
            raise ValueError("csv_path is required to append rows.")
        with self.csv_path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(
                [
                    row["date"],
                    row["ticker"],
                    row["open"],
                    row["high"],
                    row["low"],
                    row["close"],
                    row["volume"],
                ]
            )

    def _ingest_csv_to_db(self):
        if not self.persist or self.csv_path is None:
            return
        if self._ingested or self._interval is None:
            return

        df = self.read_csv(str(self.csv_path), self._interval)
        if df.is_empty():
            self._ingested = True
            return

        insert_data(
            df=df,
            db_cols=[
                "date",
                "ticker",
                "interval",
                "open",
                "high",
                "low",
                "close",
                "volume",
            ],
            table_name=self.table_name,
            conn=self.conn,
            pk_cols=["date", "ticker", "interval"],
        )
        self._ingested = True

    @staticmethod
    def _parse_timestamp(timestamp: int | float | str) -> str:
        ts = int(timestamp)
        if ts > 10_000_000_000:
            dt_obj = dt.datetime.fromtimestamp(ts / 1000, tz=dt.UTC)
        else:
            dt_obj = dt.datetime.fromtimestamp(ts, tz=dt.UTC)
        return dt_obj.isoformat()

    @staticmethod
    def read_csv(csv_path: str, interval: str) -> pl.DataFrame:
        df = pl.read_csv(csv_path)
        if df.is_empty():
            return pl.DataFrame(
                schema={
                    "date": pl.Datetime(time_zone="UTC"),
                    "ticker": pl.String,
                    "open": pl.Float64,
                    "high": pl.Float64,
                    "low": pl.Float64,
                    "close": pl.Float64,
                    "volume": pl.Int64,
                    "interval": pl.String,
                }
            )

        df = (
            df.with_columns(
                [
                    pl.col("date").str.to_datetime(strict=False, time_zone="UTC"),
                    pl.col("ticker").cast(pl.String),
                    pl.col("open").cast(pl.Float64),
                    pl.col("high").cast(pl.Float64),
                    pl.col("low").cast(pl.Float64),
                    pl.col("close").cast(pl.Float64),
                    pl.col("volume").cast(pl.Int64),
                ]
            )
            .sort(["ticker", "date"])
            .with_columns(pl.col("date").dt.date().alias("session_date"))
            .with_columns(
                (
                    pl.col("volume")
                    - pl.col("volume").shift(1).over(["ticker", "session_date"])
                )
                .fill_null(0)
                .clip(lower_bound=0)
                .alias("trade_volume")
            )
        )

        return (
            df.group_by_dynamic(
                index_column="date",
                every=interval,
                period=interval,
                group_by="ticker",
                closed="left",
                label="left",
            )
            .agg(
                [
                    pl.col("open").first().alias("open"),
                    pl.col("high").max().alias("high"),
                    pl.col("low").min().alias("low"),
                    pl.col("close").last().alias("close"),
                    pl.col("trade_volume").sum().cast(pl.Int64).alias("volume"),
                ]
            )
            .filter(pl.col("open").is_not_null())
            .with_columns(pl.lit(interval).alias("interval"))
            .select(
                ["date", "ticker", "interval", "open", "high", "low", "close", "volume"]
            )
            .sort(["ticker", "date"])
        )
