"""
An abstract base class for MarketData instances.
A MarketData class is used for fetching live and historical market data.
"""
from abc import ABC, abstractmethod
from typing import Set

import pandas as pd

from aqua.security import Stock


class IMarketData(ABC):
    """
    Abstract base class for market data.
    """

    @abstractmethod
    async def get_stocks_by_symbol(self, symbol: str) -> Set[Stock]:
        """
        Searches for a set of stocks with a given symbol.
        @return: a set of Stock instances with a given symbol.
        """

    @abstractmethod
    async def get_stock_bar_history(
        self,
        stock: Stock,
        start: pd.Timestamp,
        end: pd.Timestamp,
        bar_size: pd.Timedelta,
    ) -> pd.DataFrame:
        """
        Returns the bar history for a particular stock (unadjusted for splits)
        @return: a pandas DataFrame with columns "Open", "High", "Low", "Close", and "Volume".
        Note that some market data sources may provide extra columns such as volume weighted
        average
        """


class DataSourceError(Exception):
    """
    Data source error occurs when the data source (polygon, ibkr, etc.)
    service returns an error for a given request
    """
