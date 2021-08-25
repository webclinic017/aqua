"""
Polygon.io market data
"""

import logging
import os
import urllib.parse
from typing import Any, Mapping, Optional, Union

import aiohttp
import pandas as pd
from dotenv import load_dotenv

from aqua.market_data import errors, market_data_interface
from aqua.market_data.market_data_interface import _set_time
from aqua.security import Stock
from aqua.security.security import Security

logger = logging.getLogger(__name__)

if not load_dotenv():
    logger.warning("Can't load dotenv file")

_POLYGON_URL = os.getenv("POLYGON_URL")
_POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

if _POLYGON_URL is None:
    logger.fatal("Unable to load Polygon url")
    raise errors.ConfigError

if _POLYGON_API_KEY is None:
    logger.fatal("Can't load polygon api key")
    raise errors.CredentialError


class PolygonMarketData(market_data_interface.IMarketData):
    """
    Polygon market data gets market data from polygon.io asynchronously
    """

    def __init__(self) -> None:
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self) -> "PolygonMarketData":
        self.session = aiohttp.ClientSession(
            headers={"Authorization": f"Bearer {_POLYGON_API_KEY}"}
        )
        return self

    async def __aexit__(self, *exec_info) -> None:
        await self.session.close()
        self.session = None

    @property
    def name(self) -> str:
        return "Polygon.io"

    async def _get(self, path: str, params: Optional[Mapping[str, str]] = None) -> Any:
        url = urllib.parse.urljoin(_POLYGON_URL, path)
        async with self.session.get(url, params=params) as response:
            if response.status != 200:
                logger.warning(
                    "Got error %s, %s", response.status, await response.json()
                )
                if response.status == 429:
                    logger.warning("Rate limited")
                    raise errors.RateLimitError
                raise errors.DataSourceError
            return await response.json()

    async def _get_stock_hist_bars(
        self,
        symbol: str,
        multiplier: int,
        timespan: str,
        from_date: str,
        to_date: str,
    ) -> Union[pd.DataFrame, type(NotImplemented)]:
        """Assumes from_date and to_date do not need to be URL escaped"""
        # https://polygon.io/docs/get_v2_aggs_ticker__stocksTicker__range__multiplier___timespan___from___to__anchor
        symbol = urllib.parse.quote(symbol)
        response = await self._get(
            f"/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from_date}/{to_date}",
            params={
                "adjusted": "false",
                "limit": "50000",
            },
        )
        if response["status"] == "DELAYED":
            raise errors.DataPermissionError
        if "results" not in response:
            return pd.DataFrame()
        return pd.DataFrame(response["results"])

    async def get_hist_bars(
        self,
        security: Security,
        bar_size: pd.Timedelta,
        start_date: pd.Timestamp,
        end_date: Optional[pd.Timestamp] = None,
    ) -> Union[pd.DataFrame, type(NotImplemented)]:
        # validate security
        if not isinstance(security, Stock):
            logger.warning(
                "Polygon.io only supports stock historical bars. Got security type: %s",
                type(security),
            )
            return NotImplemented
        # validate start and end dates
        if end_date is None:
            end_date = start_date
        start_date = _set_time(start_date).floor("D")
        end_date = _set_time(end_date).floor("D")
        if end_date < start_date:
            raise ValueError("End date cannot come before start date")
        # validate bar_size and generate periods to query based on bar_size
        if bar_size.value <= 0:
            raise ValueError(f"Got non positive bar_size: {bar_size}")
        if bar_size % pd.Timedelta("1 day") == pd.Timedelta(0):
            range_multiplier = bar_size // pd.Timedelta("1 day")
            timespan = "day"
            period_freq = pd.DateOffset(days=50000)
        elif bar_size % pd.Timedelta("1 min") == pd.Timedelta(0):
            range_multiplier = bar_size // pd.Timedelta("1 min")
            timespan = "minute"
            period_freq = pd.DateOffset(days=75)
        else:
            logger.warning("Bar size %s not supported", bar_size)
            return NotImplemented
        periods = list(pd.date_range(start_date, end_date, freq=period_freq))
        periods.append(end_date + pd.DateOffset(days=1))
        # make requests for each period
        res = []
        for i in range(len(periods) - 1):
            period_start = periods[i]
            period_end = periods[i + 1] - pd.DateOffset(days=1)
            response = await self._get_stock_hist_bars(
                security.symbol,
                range_multiplier,
                timespan,
                period_start.strftime("%Y-%m-%d"),
                period_end.strftime("%Y-%m-%d"),
            )
            if response is NotImplemented:
                return NotImplemented
            if response.empty:
                continue
            res.append(response)
        if len(res) == 0:
            return pd.DataFrame(
                columns=["Open", "High", "Low", "Close", "Volume", "NumTrades", "VWAP"]
            )
        res = pd.concat(res)
        res.rename(
            columns={
                "c": "Close",
                "h": "High",
                "l": "Low",
                "n": "NumTrades",
                "o": "Open",
                "t": "Time",
                "v": "Volume",
                "vw": "VWAP",
            },
            inplace=True,
        )
        res["Time"] = res["Time"].map(
            lambda x: pd.Timestamp(x, unit="ms", tz="America/New_York")
        )
        res.set_index("Time", inplace=True)
        res.sort_index(inplace=True)
        return res[["Open", "High", "Low", "Close", "Volume", "NumTrades", "VWAP"]]

    async def get_stock_dividends(
        self, stock: Stock
    ) -> Union[pd.DataFrame, type(NotImplemented)]:
        path = f"/v2/reference/dividends/{urllib.parse.quote_plus(stock.symbol)}"
        response = await self._get(path)
        res = pd.DataFrame(response["results"])
        res.rename(
            columns={
                "amount": "Amount",
                "exDate": "ExDate",
                "paymentDate": "PaymentDate",
                "recordDate": "RecordDate",
            },
            inplace=True,
        )
        return res[["Amount", "ExDate", "PaymentDate", "RecordDate"]]

    async def get_stock_splits(
        self, stock: Stock
    ) -> Union[pd.DataFrame, type(NotImplemented)]:
        path = f"/v2/reference/splits/{urllib.parse.quote_plus(stock.symbol)}"
        response = await self._get(path)
        res = pd.DataFrame(response["results"])
        res.rename(columns={"ratio": "Ratio", "exDate": "ExDate"}, inplace=True)
        return res[["Ratio", "ExDate"]]
