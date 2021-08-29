"""
The "main" market data class that uses all the market data implementations together to load balance
each data source.
"""
import contextlib
import logging
from typing import Optional, Union

import pandas as pd

from aqua.market_data import errors
from aqua.market_data.market_data_interface import IMarketData, IStreamingMarketData
from aqua.security import Stock
from aqua.security.security import Security

logger = logging.getLogger(__name__)

_market_data: list[IMarketData] = []
try:
    from aqua.market_data.alpaca import AlpacaMarketData

    _market_data.append(AlpacaMarketData)
except (errors.ConfigError, errors.CredentialError) as import_exception:
    logger.warning("Can't import Alpaca: %s", import_exception)

try:
    from aqua.market_data.polygon import PolygonMarketData

    _market_data.append(PolygonMarketData)
except (errors.ConfigError, errors.CredentialError) as import_exception:
    logger.warning("Can't import Polygon: %s", import_exception)

try:
    from aqua.market_data.ibkr import IBKRMarketData

    _market_data.append(IBKRMarketData)
except (errors.ConfigError, errors.CredentialError) as import_exception:
    logger.warning("Can't import IBKR: %s", import_exception)


class MarketData(IMarketData):
    """
    The main market data implementation that uses an aggregation of each market data
    """

    def __init__(self):
        self._context = contextlib.AsyncExitStack()

    async def __aenter__(self):
        await self._context.__aenter__()
        for market_data in _market_data:
            await self._context.enter_async_context(market_data)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._context.__aexit__(exc_type, exc_val, exc_tb)

    @property
    def name(self) -> str:
        return (
            "MarketData("
            + ",".join(market_data.name for market_data in _market_data)
            + ")"
        )

    async def get_hist_bars(
        self,
        security: Security,
        bar_size: pd.Timedelta,
        start_date: pd.Timestamp,
        end_date: Optional[pd.Timestamp] = None,
    ) -> Union[pd.DataFrame, type(NotImplemented)]:
        for market_data in _market_data:
            try:
                res = await market_data.get_hist_bars(
                    security, bar_size, start_date, end_date
                )
                if res is not NotImplemented:
                    return res
            except Exception as exception:  # pylint: disable=broad-except
                logger.info("%s failed get_hist_bar: %s", market_data.name, exception)
        return NotImplemented

    def get_streaming_market_data(
        self,
    ) -> Union[IStreamingMarketData, type(NotImplemented)]:
        for market_data in _market_data:
            try:
                res = market_data.get_streaming_market_data()
                if res is not NotImplemented:
                    return res
            except Exception as exception:  # pylint: disable=broad-except
                logger.info(
                    "%s failed get_streaming_market_data: %s",
                    market_data.name,
                    exception,
                )
        return NotImplemented

    async def get_stock_dividends(
        self, stock: Stock
    ) -> Union[pd.DataFrame, type(NotImplemented)]:
        for market_data in _market_data:
            try:
                res = await market_data.get_stock_dividends(stock)
                if res is not NotImplemented:
                    return res
            except Exception as exception:  # pylint: disable=broad-except
                logger.info(
                    "%s failed get_stock_dividends: %s", market_data.name, exception
                )
        return NotImplemented

    async def get_stock_splits(
        self, stock: Stock
    ) -> Union[pd.DataFrame, type(NotImplemented)]:
        for market_data in _market_data:
            try:
                res = await market_data.get_stock_splits(stock)
                if res is not NotImplemented:
                    return res
            except Exception as exception:  # pylint: disable=broad-except
                logger.info(
                    "%s failed get_stock_splits: %s", market_data.name, exception
                )
        return NotImplemented
