# pylint: disable=unused-argument
"""
An abstract base class for MarketData instances.
A MarketData class is used for fetching live and historical market data.
"""
from abc import ABC, abstractmethod
from typing import Optional, Set, Tuple, Union

import pandas as pd

from aqua.security import Stock
from aqua.security.security import Security


class IMarketData(ABC):
    """
    Abstract base class for market data. Any functionality that's not supported will return
    `NotImplemented`.

    All Timestamps without a localized timezone will be treated as America/New_York time.
    """

    async def __aenter__(self) -> "IMarketData":
        """
        Sets up the market data connections
        """

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Closes market data connections
        """

    # Metadata
    # --------

    @property
    @abstractmethod
    def name(self) -> str:
        """Returns a (unique) name for the market data"""

    # Price Data
    # ----------

    async def get_hist_bars(
        self,
        security: Security,
        bar_size: pd.Timedelta,
        start_date: pd.Timestamp,
        end_date: Optional[pd.Timestamp] = None,
    ) -> Union[pd.DataFrame, type(NotImplemented)]:
        """
        Gets historical bars for trades made on a given security over a time span. The trades
        are not adjusted for splits or dividends. Note that the start and end dates are inclusive.
        This means all trades made on `start_date` and `end_date` are considered as part of the
        aggregation.

        Returns pandas DataFrame with columns "Open", "High", "Low", "Close", "Volume",
        "NumTrades", and "VWAP". Some data sources may include more columns. If the parameters
        cannot be satisfied, `NotImplemented` is returned instead.

        The index of the DataFrame marks the start time of each bar.

        :param security: the security to get historical bars for
        :param start_date: the start date for trades. Note that the Timestamp is first converted
            to timezone America/New_York (or localized) and then rounded down to the latest day
            before the timestamp.
        :param end_date: the end date for trades (inclusive). The same timezone rounding rules for
            `start_date` applies to `end_date`. If not specified, then the end_date is assumed to
            be the same as the start date (so only 1 day of data will be fetched)
        :param bar_size: the duration of each bar
        :return: a pandas DataFrame with columns "Open", "High", "Low", "Close", "Volume",
            "NumTrades", and "VWAP" and index "Time". If the data source does not support the
            parameters specified, `NotImplemented` will be returned instead.
        :raise ValueError: if `end_date` is before `start_date` or if `bar_size` is not positive
        """
        return NotImplemented

    def get_streaming_market_data(  # pylint: disable=no-self-use
        self,
    ) -> Union["IStreamingMarketData", type(NotImplemented)]:
        """Returns an IStreamingMarketData for live market data (if supported)"""
        return NotImplemented

    # Fundamental Data
    # ----------------

    async def get_stock_dividends(
        self, stock: Stock
    ) -> Union[pd.DataFrame, type(NotImplemented)]:
        """
        Returns the dividend history of a stock as a pandas DataFrame

        The DataFrame has columns "Amount", "ExDate", "PaymentDate", and "RecordDate"

        :param stock: the stock to fetch dividends for
        :return: a pandas DataFrame
        """
        return NotImplemented

    async def get_stock_splits(
        self, stock: Stock
    ) -> Union[pd.DataFrame, type(NotImplemented)]:
        """
        Returns all stock splits as a pandas DataFrame.

        The DataFrame has columns "Ratio" and "ExDate"

        :param stock: the stock to fetch splits for
        :return: a pandas DataFrame
        """
        return NotImplemented


class IStreamingMarketData:
    """
    Base class for streaming live market data
    """

    async def __aenter__(self) -> "IStreamingMarketData":
        """
        Sets up the market data connections. This usually creates a websocket connection and
        authenticates
        """

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Closes market data connections
        """

    async def subscribe_trades(
        self, security: Security
    ) -> Union[None, type(NotImplemented)]:
        """
        Subscribes to trades made on a security

        :param security: security to subscribe to
        :return: True on success, False otherwise
        """
        return NotImplemented

    async def unsubscribe_trades(
        self, security: Security
    ) -> Union[None, type(NotImplemented)]:
        """
        Unsubscribes from trades for a security

        :param security: the security to unsubscribe
        :return: returns None if security supports unsubscribe. returns `NotImplemented` otherwise
        """
        return NotImplemented

    async def get_trade(self, security: Security) -> Tuple[float, int, pd.Timestamp]:
        """
        Asynchronously gets the next available trade for a security. This method will block until
        a new trade is received for the given security but only asynchronously (i.e. if no trades
        are available, it must relinquish control of the event loop somehow)

        :param security:
        :return: a tuple (trade price, trade size, trade time)
        :raise ValueError: if the security has not been subscribed to. See method `subscribe_trades`
        """
        return NotImplemented

    @property
    @abstractmethod
    async def trade_subscriptions(self) -> Set[Security]:
        """
        Returns a set of securities that the market data is subscribed to
        :return: a set of securities
        """

    async def subscribe_quotes(
        self, security: Security
    ) -> Union[None, type(NotImplemented)]:
        """
        Subscribes to quotes made on a security

        :param security: security to subscribe to
        :return: True on success, False otherwise
        """
        return NotImplemented

    async def unsubscribe_quotes(
        self, security: Security
    ) -> Union[None, type(NotImplemented)]:
        """
        Unsubscribes from quotes for a security

        :param security: the security to unsubscribe
        :return: returns None if security supports unsubscribe. returns `NotImplemented` otherwise
        """
        return NotImplemented

    async def get_quote(
        self, security: Security
    ) -> Tuple[Tuple[float, int], Tuple[float, int]]:
        """
        Asynchronously gets the next available quote for a security. This method will block until
        a new quote is received for the given security but only asynchronously (i.e. if no quotes
        are available, it must relinquish control of the event loop somehow)

        :param security:
        :return: a tuple ((bid price, bid size), (offer price, offer size))
        :raise ValueError: if the security has not been subscribed to. See method `subscribe_trades`
        """
        return NotImplemented

    @property
    @abstractmethod
    async def quote_subscriptions(self) -> Set[Security]:
        """
        Returns a set of securities that the market data is subscribed to for quotes
        :return: a set of securities
        """


def _set_time(time: pd.Timestamp) -> pd.Timestamp:
    """Convert or localize timezone to New York"""
    if time.tz is None:
        return time.tz_localize("America/New_York")
    return time.tz_convert("America/New_York")
