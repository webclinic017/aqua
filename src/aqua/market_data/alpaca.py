"""
Alpaca market data
"""

import logging
import os
import urllib.parse
from typing import Set

import aiohttp
import pandas as pd
from dotenv import load_dotenv

from aqua.market_data import errors, market_data_interface
from aqua.security import Stock

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
        self.session = aiohttp.ClientSession(
            headers={
                "APCA-API-KEY-ID": _ALPACA_KEY_ID,
                "APCA-API-SECRET-KEY": _ALPACA_SECRET_KEY,
            }
        )

    async def __aenter__(self) -> "AlpacaMarketData":
        return self

    async def __aexit__(self, *exec_info):
        await self.session.close()

    async def get_stocks_by_symbol(self, symbol: str) -> Set[Stock]:
        path = f"/v2/assets/{urllib.parse.quote_plus(symbol.upper())}"
        url = urllib.parse.urljoin(_ALPACA_URL, path)
        async with self.session.get(url) as response:
            if response.status != 200:
                raise errors.DataSourceError
            response = await response.json()
            if "symbol" in response:
                return {Stock(response["symbol"])}
        return set()

    async def get_stock_bar_history(
        self,
        stock: Stock,
        start: pd.Timestamp,
        end: pd.Timestamp,
        bar_size: pd.Timedelta,
    ) -> pd.DataFrame:
        if bar_size == pd.Timedelta(1, unit="min"):
            timeframe = "1Min"
        elif bar_size == pd.Timedelta(1, unit="hr"):
            timeframe = "1Hour"
        elif bar_size == pd.Timedelta(1, unit="day"):
            timeframe = "1Day"
        else:
            raise ValueError(
                "Alpaca only supports bar sizes of 1 min, 1 hour, or 1 day"
            )
        end += bar_size  # make sure we include the last bar
        if start.tz is None:
            start = start.tz_localize("America/New_York")
        if end.tz is None:
            end = end.tz_localize("America/New_York")
        path = f"/v2/stocks/{urllib.parse.quote_plus(stock.symbol)}/bars"
        res = list()
        url_params = {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "limit": 10000,
            "timeframe": timeframe,
        }
        while url_params is not None:
            url = (
                urllib.parse.urljoin(_ALPACA_DATA_URL, path)
                + "?"
                + urllib.parse.urlencode(url_params, safe=":")
            )
            async with self.session.get(url) as response:
                if response.status != 200:
                    raise errors.DataSourceError
                response = await response.json()
                if "bars" in response:
                    res.append(pd.DataFrame(response["bars"]))
                else:
                    logger.info("Got empty response for request %s", url)
                url_params["page_token"] = response["next_page_token"]
                if url_params["page_token"] is None:
                    url_params = None
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
