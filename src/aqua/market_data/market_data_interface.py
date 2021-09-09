# pylint: disable=unused-argument
"""
An abstract base class for MarketData instances.
A MarketData class is used for fetching live and historical market data.
"""
import enum
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Union

import pandas as pd

from aqua.security import Stock
from aqua.security.security import Security


class StreamType(enum.Enum):
    """The stream type for market data subscriptions"""

    QUOTES = "quotes"
    TRADES = "trades"


@dataclass
class Trade:
    """A single trade"""

    price: float
    size: int
    time: pd.Timestamp

    def __post_init__(self):
        if self.size < 0:
            raise ValueError("Trade size can't be negative")


@dataclass
class Quote:
    """A single quote"""

    bid: float
    bid_size: int
    ask: float
    ask_size: int
    time: pd.Timestamp

    def __post_init__(self):
        if self.bid_size < 0 or self.ask_size < 0:
            raise ValueError(
                f"Bid and ask size can't be negative. Got {self.bid_size} x {self.ask_size}"
            )

    def mid_price(self) -> float:
        """
        Calculates the size weighted mid price of a quote. Returns nan if quote isn't double sided
        """
        if self.bid_size or self.ask_size == 0:
            return float("nan")
        return (self.bid * self.ask_size + self.ask * self.bid_size) / (
            self.bid_size + self.ask_size
        )


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

    # Streaming data
    # --------------

    async def subscribe(
        self, stream_type: StreamType, security: Security
    ) -> Union[None, type(NotImplemented)]:
        """
        Subscribes to a stream of a given `stream_type` for a security. Calling subscribe on the
        same security multiple twice does nothing. Call `unsubscribe(...)` followed by
        `subscribe(...)` to refresh a subscription.

        :param stream_type: what to subscribe
        :param security: the security to subscribe to
        :return: None if subscription was successful. If the given stream type cannot be subscribed
            to for the given security, `NotImplemented` is returned.
        """
        return NotImplemented

    async def get(
        self, stream_type: StreamType, security: Security
    ) -> Union[Quote, Trade]:
        """
        Asynchronously gets the next value for a security subscription. Note that subscriptions
        can cause values to "pile up" if `get(...)` is not called frequent enough.

        :param stream_type: the stream type to get the next value for
        :param security: the security to get the next value for
        :return: depending on `stream_type`, a quote or trade object
        :raise ValueError: if the security was not subscribed to
        :raise NotImplementedError: if the stream type is not supported. (this doesn't return
            NotImplemented because the client should never call this before calling
            `subscribe(...)`)
        """
        raise NotImplementedError

    async def unsubscribe(self, stream_type: StreamType, security: Security) -> None:
        """
        Unsubscribes from a security. This will clear the internal buffer for incoming
        values.

        :param stream_type: the stream type to subscribe to
        :param security: the security to unsubscribe from
        :raise ValueError: if the security was not subscribed to
        """
        raise NotImplementedError

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


def _set_time(time: pd.Timestamp) -> pd.Timestamp:
    """Convert or localize timezone to New York"""
    if time.tz is None:
        return time.tz_localize("America/New_York")
    return time.tz_convert("America/New_York")
