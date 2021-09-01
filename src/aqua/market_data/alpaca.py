"""
Alpaca market data
"""
import asyncio
import logging
import os
import urllib.parse
from typing import Any, Iterable, Mapping, Optional, Union

import aiohttp
import pandas as pd
from dotenv import load_dotenv

from aqua.market_data import errors, market_data_interface
from aqua.market_data.alpaca_stream import AlpacaStreamingMarketData
from aqua.market_data.market_data_interface import _set_time
from aqua.security import Stock
from aqua.security.security import Security

logger = logging.getLogger(__name__)

if not load_dotenv():
    logger.warning("Can't load dotenv file")

_ALPACA_URL = os.getenv("ALPACA_URL")
_ALPACA_DATA_URL = os.getenv("ALPACA_DATA_URL")
_ALPACA_KEY_ID = os.getenv("ALPACA_KEY_ID")
_ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")

if _ALPACA_URL is None or _ALPACA_DATA_URL is None:
    logger.fatal("Unable to load Alpaca urls")
    raise errors.ConfigError

if _ALPACA_KEY_ID is None or _ALPACA_SECRET_KEY is None:
    logger.fatal("Can't load alpaca keys")
    raise errors.CredentialError


class AlpacaMarketData(market_data_interface.IMarketData):
    """
    Alpaca market data gets market data from alpaca asynchronously
    """

    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self) -> "AlpacaMarketData":
        self.session = aiohttp.ClientSession(
            headers={
                "APCA-API-KEY-ID": _ALPACA_KEY_ID,
                "APCA-API-SECRET-KEY": _ALPACA_SECRET_KEY,
            }
        )
        return self

    async def __aexit__(self, *exec_info):
        await self.session.close()
        await asyncio.sleep(
            0.25
        )  # https://docs.aiohttp.org/en/stable/client_advanced.html#graceful-shutdown
        self.session = None

    @property
    def name(self) -> str:
        return "Alpaca"

    async def _get(
        self, path: str, params: Optional[Mapping[str, str]] = None
    ) -> Iterable[Any]:
        url = urllib.parse.urljoin(_ALPACA_DATA_URL, path)
        if params is None:
            params = {}
        params["limit"] = "10000"
        responses = []
        has_next = True
        while has_next:
            async with self.session.get(url, params=params) as response:
                if response.status != 200:
                    logger.warning(
                        "Got error %s: %s", response.status, await response.json()
                    )
                    if response.status == 429:
                        logger.warning("Rate limited")
                        raise errors.RateLimitError
                    if response.status == 422:
                        json = await response.json()
                        if json.get("code", -1) == 42210000:
                            # subscription does not permit querying data from the past 15 minutes
                            raise errors.DataPermissionError
                    raise errors.DataSourceError
                response = await response.json()
                responses.append(response)
                next_page_token = response.get("next_page_token", None)
                if next_page_token is not None:
                    params["next_page_token"] = next_page_token
                else:
                    has_next = False
        return responses

    async def _get_stock_hist_bars(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        timeframe: str,
    ) -> pd.DataFrame:
        """Assumes start_date and end_date do not need to be URL escaped"""
        responses = await self._get(
            f"/v2/stocks/{urllib.parse.quote(symbol)}/bars",
            params={"start": start_date, "end": end_date, "timeframe": timeframe},
        )
        res = []
        for response in responses:
            if "bars" in response:
                res.append(pd.DataFrame(response["bars"]))
        res = pd.concat(res)
        return res

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
        if bar_size == pd.Timedelta("1 min"):
            timeframe = "1Min"
            period_freq = pd.DateOffset(days=10)
        elif bar_size == pd.Timedelta("1 hr"):
            timeframe = "1Hour"
            period_freq = pd.DateOffset(days=600)
        elif bar_size == pd.Timedelta("1 day"):
            timeframe = "1Day"
            period_freq = pd.DateOffset(days=9000)
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
                period_start.strftime("%Y-%m-%d"),
                period_end.strftime("%Y-%m-%d"),
                timeframe,
            )
            if response.empty:
                continue
            res.append(response)
        res = pd.concat(res)
        res.rename(
            columns={
                "t": "Time",
                "o": "Open",
                "h": "High",
                "l": "Low",
                "c": "Close",
                "v": "Volume",
                "n": "NumTrades",
                "vw": "VWAP",
            },
            inplace=True,
        )
        res["Time"] = res["Time"].map(lambda x: pd.Timestamp(x, tz="America/New_York"))
        res.set_index("Time", inplace=True)
        res.sort_index(inplace=True)
        return res

    def get_streaming_market_data(
        self,
    ) -> Union["AlpacaStreamingMarketData", type(NotImplemented)]:
        return AlpacaStreamingMarketData()
