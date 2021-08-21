# pylint: disable=import-outside-toplevel
"""
The aggregate market data uses multiple data sources (Alpaca, Polygon, IBKR, etc.) to provide
a single interface for all market data related API calls
"""
import logging
from typing import Any, Awaitable, Callable, Set

import pandas as pd

from aqua.market_data import errors
from aqua.market_data.market_data_interface import IMarketData
from aqua.security import Option, Stock

logger = logging.getLogger(__name__)


class MarketData(IMarketData):
    """
    MarketData uses all other market data classes in this module to provide
    the fullest set of supported API calls
    """

    def __init__(self):
        self.valid_data_sources: list[IMarketData] = []
        self.data_sources: list[IMarketData] = []
        try:
            from aqua.market_data.ibkr import IBKRMarketData

            self.data_sources.append(IBKRMarketData())
        except (errors.ConfigError, errors.CredentialError) as exception:
            logger.warning("Couldn't get IBKRMarketData: %s", exception)
        try:
            from aqua.market_data.polygon import PolygonMarketData

            self.data_sources.append(PolygonMarketData())
        except (errors.ConfigError, errors.CredentialError) as exception:
            logger.warning("Couldn't get PolygonMarketData: %s", exception)
        try:
            from aqua.market_data.alpaca import AlpacaMarketData

            self.data_sources.append(AlpacaMarketData())
        except (errors.ConfigError, errors.CredentialError) as exception:
            logger.warning("Couldn't get AlpacaMarketData: %s", exception)

    async def __aenter__(self):
        for data in self.data_sources:
            try:
                await data.__aenter__()
                self.valid_data_sources.append(data)
            except Exception as exception:  # pylint: disable=broad-except
                logger.info(
                    "Skipped entering context %s because of exception: %s",
                    data,
                    exception,
                )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        for data in self.valid_data_sources:
            await data.__aexit__(exc_type, exc_val, exc_tb)
        self.valid_data_sources = []

    async def get_stocks_by_symbol(self, symbol: str) -> Set[Stock]:
        return await self._try_apply(lambda x: x.get_stocks_by_symbol(symbol))

    async def get_stock_bar_history(
        self,
        stock: Stock,
        start: pd.Timestamp,
        end: pd.Timestamp,
        bar_size: pd.Timedelta,
    ) -> pd.DataFrame:
        return await self._try_apply(
            lambda x: x.get_stock_bar_history(stock, start, end, bar_size)
        )

    async def get_stock_dividends(self, stock: Stock) -> pd.DataFrame:
        return await self._try_apply(lambda x: x.get_stock_dividends(stock))

    async def get_stock_splits(self, stock: Stock) -> pd.DataFrame:
        return await self._try_apply(lambda x: x.get_stock_splits(stock))

    async def get_option_bar_history(
        self,
        option: Option,
        start: pd.Timestamp,
        end: pd.Timestamp,
        bar_size: pd.Timedelta,
    ) -> pd.DataFrame:
        return await self._try_apply(
            lambda x: x.get_option_bar_history(option, start, end, bar_size)
        )

    async def _try_apply(self, data_fn: Callable[[IMarketData], Awaitable[Any]]) -> Any:
        for data in self.valid_data_sources:
            try:
                res = await data_fn(data)
                if res is not NotImplemented:
                    return res
            except Exception as exception:  # pylint: disable=broad-except
                logger.info("Skipping %s due to exception: %s", data, exception)
        return NotImplemented
