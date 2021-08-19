"""
Polygon.io market data
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
        self.session = aiohttp.ClientSession(
            headers={"Authorization": f"Bearer {_POLYGON_API_KEY}"}
        )

    async def __aenter__(self) -> "PolygonMarketData":
        return self

    async def __aexit__(self, *exec_info) -> None:
        await self.session.close()

    async def get_stocks_by_symbol(self, symbol: str) -> Set[Stock]:
        path = "/v3/reference/tickers"
        next_url = (
            urllib.parse.urljoin(_POLYGON_URL, path)
            + "?"
            + urllib.parse.urlencode({"ticker": symbol.upper()})
        )
        results = set()
        while next_url is not None:
            logger.debug("Loading %s", next_url)
            async with self.session.get(next_url) as response:
                if response.status != 200:
                    raise errors.DataSourceError
                response = await response.json()
                for stock in response["results"]:
                    results.add(Stock(stock["ticker"]))
                if "next_url" in response:
                    logger.debug("Found next url")
                    next_url = response["next_url"]
                else:
                    next_url = None
        return results

    async def get_stock_bar_history(
        self,
        stock: Stock,
        start: pd.Timestamp,
        end: pd.Timestamp,
        bar_size: pd.Timedelta,
    ) -> pd.DataFrame:
        if bar_size.total_seconds() % pd.Timedelta(1, unit="day").total_seconds() == 0:
            range_multiplier = bar_size.days
            units = "day"
        else:
            range_multiplier = int(round(bar_size.total_seconds() / 60))
            units = "minute"
        raw_res = list()

        if units == "day":
            periods = list(pd.date_range(start, end, freq=pd.DateOffset(days=50000)))
        else:
            assert units == "minute"
            periods = list(pd.date_range(start, end, freq=pd.DateOffset(days=75)))
        periods.append(end + pd.DateOffset(days=1))
        logger.debug("Broke up request into %d periods", len(periods))
        for i in range(len(periods) - 1):
            period_start = periods[i]
            period_end = periods[i + 1] - pd.DateOffset(days=1)
            logger.debug(
                "Fetching period %s to %s",
                period_start.strftime("%Y-%m-%d"),
                period_end.strftime("%Y-%m-%d"),
            )
            url = (
                urllib.parse.urljoin(
                    _POLYGON_URL,
                    (
                        "/v2/aggs"
                        + f"/ticker/{stock.symbol}"
                        + f"/range/{range_multiplier}/{units}"
                        + f"/{period_start.strftime('%Y-%m-%d')}"
                        + f"/{period_end.strftime('%Y-%m-%d')}"
                    ),
                )
                + "?"
                + urllib.parse.urlencode({"limit": 50000, "adjusted": False})
            )
            async with self.session.get(url) as response:
                if response.status != 200:
                    logger.warning("Error: %s", await response.json())
                    raise errors.DataSourceError
                response = await response.json()
                if "results" in response:
                    raw_res.append(pd.DataFrame(response["results"]))
                else:
                    logger.info("Got empty response for request %s", url)
                    raw_res.append(pd.DataFrame())

        raw_res = pd.concat(raw_res)
        if raw_res.empty:
            return pd.DataFrame(
                columns=["Open", "High", "Low", "Close", "Volume", "NumTrades", "VWAP"]
            )
        raw_res["Time"] = raw_res["t"].map(
            lambda x: pd.Timestamp(x, unit="ms", tz="America/New_York")
        )
        raw_res.set_index("Time", inplace=True)
        res = pd.DataFrame(
            {
                "Open": raw_res["o"],
                "High": raw_res["h"],
                "Low": raw_res["l"],
                "Close": raw_res["c"],
                "Volume": raw_res["v"],
                "NumTrades": raw_res["n"],
                "VWAP": raw_res["vw"],
            }
        )
        return res

    async def get_stock_dividends(self, stock: Stock) -> pd.DataFrame:
        path = f"/v2/reference/dividends/{urllib.parse.quote_plus(stock.symbol)}"
        url = urllib.parse.urljoin(_POLYGON_URL, path)
        async with self.session.get(url) as response:
            if response.status != 200:
                raise errors.DataSourceError
            response = await response.json()
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

    async def get_stock_splits(self, stock: Stock) -> pd.DataFrame:
        path = f"/v2/reference/splits/{urllib.parse.quote_plus(stock.symbol)}"
        url = urllib.parse.urljoin(_POLYGON_URL, path)
        async with self.session.get(url) as response:
            if response.status != 200:
                raise errors.DataSourceError
            response = await response.json()
            res = pd.DataFrame(response["results"])
        res.rename(columns={"ratio": "Ratio", "exDate": "ExDate"}, inplace=True)
        return res[["Ratio", "ExDate"]]
