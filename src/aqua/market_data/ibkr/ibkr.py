"""
IBKR market data using official Python library
"""
import asyncio
import copy
import logging
from collections import defaultdict
from contextlib import ExitStack
from typing import Optional, Union

import pandas as pd
from ibapi.common import BarData, TickAttrib, TickerId
from ibapi.ticktype import TickType

from aqua.internal.ibkr import IBKRBase, security_to_ibkr_contract
from aqua.market_data import errors
from aqua.market_data.market_data_interface import (
    IMarketData,
    Quote,
    StreamType,
    Trade,
    _set_time,
)
from aqua.security.security import Security

logger = logging.getLogger(__name__)


class IBKRMarketData(
    IBKRBase, IMarketData
):  # pylint: disable=too-many-instance-attributes
    """
    IBKR market data from Interactive Brokers
    """

    def __init__(self):
        IBKRBase.__init__(self, client_id=1)
        self.req_id = 1
        self.hist_bar_req_queue: dict[int, asyncio.Queue] = {}
        self.mkt_data_security_to_req_id: dict[Security, int] = {}
        self.mkt_data_req_id_to_security: dict[int, Security] = {}
        self.trades: defaultdict[Security, Trade] = defaultdict(
            lambda: Trade(0, 0, pd.Timestamp.now(tz="America/New_York"))
        )
        self.trade_subscriptions: dict[Security, asyncio.Queue[Trade]] = {}
        self.quotes: defaultdict[Security, Quote] = defaultdict(
            lambda: Quote(0, 0, 0, 0, pd.Timestamp.now(tz="America/New_York"))
        )
        self.quote_subscriptions: dict[Security, asyncio.Queue[Quote]] = {}

    async def __aenter__(self):
        await IBKRBase.__aenter__(self)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await IBKRBase.__aexit__(self, exc_type, exc_val, exc_tb)
        self.req_id = 1
        self.hist_bar_req_queue.clear()
        self.trade_subscriptions.clear()
        self.quote_subscriptions.clear()

    @property
    def name(self) -> str:
        return "IBKR"

    async def get_hist_bars(
        self,
        security: Security,
        bar_size: pd.Timedelta,
        start_date: pd.Timestamp,
        end_date: Optional[pd.Timestamp] = None,
    ) -> Union[pd.DataFrame, type(NotImplemented)]:
        # validate security
        con = security_to_ibkr_contract(security)
        if con is None:
            logger.warning(
                "Hist bars for security type: %s not supported", type(security)
            )
            return NotImplemented
        # validate start and end dates
        if end_date is None:
            end_date = start_date
        start_date = _set_time(start_date).floor("D")
        end_date = _set_time(end_date).floor("D") + pd.DateOffset(
            days=1
        )  # make end date inclusive
        if end_date <= start_date:
            raise ValueError(
                "End date must be strictly (at least 1 day) after start date"
            )
        duration_str = _time_delta_to_duration_str(end_date - start_date)
        # validate bar_size
        if bar_size.value <= 0:
            raise ValueError(f"Got non positive bar_size: {bar_size}")
        bar_size_str = _time_delta_to_bar_size_str(bar_size)
        if bar_size_str is None:
            logger.warning("Bar size %s not supported", bar_size)
            return NotImplemented
        # make request
        req_id = self.req_id
        self.req_id += 1
        logger.info(
            "Request historical data: %s %s %s %s",
            con,
            end_date.strftime("%Y%m%d %H:%M:%S"),
            duration_str,
            bar_size_str,
        )
        self.client.reqHistoricalData(
            reqId=req_id,
            contract=con,
            endDateTime=end_date.strftime("%Y%m%d %H:%M:%S"),
            durationStr=duration_str,
            barSizeSetting=bar_size_str,
            whatToShow="TRADES",
            useRTH=0,
            formatDate=1,
            keepUpToDate=False,
            chartOptions=[],
        )
        bar_queue = asyncio.Queue()
        self.hist_bar_req_queue[req_id] = bar_queue
        bars = []
        with ExitStack() as exit_stack:
            exit_stack.callback(self.hist_bar_req_queue.__delitem__, req_id)
            while (bar := await bar_queue.get()) is not None:
                if bar is NotImplemented:
                    return NotImplemented
                if isinstance(bar, Exception):
                    raise bar
                if isinstance(bar, BarData):
                    bars.append(
                        {
                            "Time": bar.date,
                            "Open": bar.open,
                            "High": bar.high,
                            "Low": bar.low,
                            "Close": bar.close,
                            "Volume": bar.volume,
                            "NumTrades": bar.barCount,
                            "VWAP": bar.average,
                        }
                    )
                else:
                    logger.error("req queue received unexpected type: %s", type(bar))
                    raise errors.DataSourceError
        res = pd.DataFrame(bars)
        try:
            res["Time"] = pd.to_datetime(res["Time"], format="%Y%m%d  %H:%M:%S")
        except ValueError:
            res["Time"] = pd.to_datetime(res["Time"], format="%Y%m%d")
        res = res.set_index("Time").sort_index()
        res.index = res.index.tz_localize("America/New_York")
        res = res.loc[slice(start_date, None, None)]
        return res

    async def subscribe(
        self, stream_type: StreamType, security: Security
    ) -> Union[None, type(NotImplemented)]:
        con = security_to_ibkr_contract(security)
        if con is None:
            logger.warning("IBKR doesn't support security type: %s", type(security))
            return NotImplemented
        if stream_type == StreamType.QUOTES:
            if security not in self.quote_subscriptions:
                self.quote_subscriptions[security] = asyncio.Queue()
        elif stream_type == StreamType.TRADES:
            if security not in self.trade_subscriptions:
                self.trade_subscriptions[security] = asyncio.Queue()
        else:
            logger.warning(
                "IBKR market data doesn't support stream type %s", stream_type
            )
            return NotImplemented
        if security not in self.mkt_data_security_to_req_id:
            # security was never subscribed to
            req_id = self.req_id
            self.req_id += 1
            self.mkt_data_security_to_req_id[security] = req_id
            self.mkt_data_req_id_to_security[req_id] = security
            self.client.reqMktData(req_id, con, "", False, False, [])

    async def get(
        self,
        stream_type: StreamType,
        security: Security,
    ) -> Union[Quote, Trade]:
        if stream_type == StreamType.QUOTES:
            if security not in self.quote_subscriptions:
                raise ValueError(f"{security} quotes never subscribed")
            quote = await self.quote_subscriptions[security].get()
            if isinstance(quote, Exception):
                raise quote
            return quote
        if stream_type == StreamType.TRADES:
            if security not in self.trade_subscriptions:
                raise ValueError(f"{security} trades never subscribed")
            trade = await self.trade_subscriptions[security].get()
            if isinstance(trade, Exception):
                raise trade
            return trade
        raise NotImplementedError

    async def unsubscribe(
        self,
        stream_type: StreamType,
        security: Security,
    ) -> None:
        if stream_type == StreamType.QUOTES:
            if security not in self.quote_subscriptions:
                raise ValueError(
                    f"{security} quotes not subscribed - can't unsubscribe"
                )
            del self.quote_subscriptions[security]
        elif stream_type == StreamType.TRADES:
            if security not in self.trade_subscriptions:
                raise ValueError(
                    f"{security} trades not subscribed - can't unsubscribe"
                )
            del self.trade_subscriptions[security]
        else:
            raise NotImplementedError
        if (
            security not in self.quote_subscriptions
            and security not in self.trade_subscriptions
        ):
            req_id = self.mkt_data_security_to_req_id[security]
            self.client.cancelMktData(req_id)
            del self.mkt_data_security_to_req_id[security]
            del self.mkt_data_req_id_to_security[req_id]

    # EWrapper methods

    def historicalData(self, reqId: int, bar: BarData):
        super().historicalData(reqId, bar)
        if reqId in self.hist_bar_req_queue:
            self.event_loop.call_soon_threadsafe(
                self.hist_bar_req_queue[reqId].put_nowait, bar
            )

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        super().historicalDataEnd(reqId, start, end)
        if reqId in self.hist_bar_req_queue:
            self.event_loop.call_soon_threadsafe(
                self.hist_bar_req_queue[reqId].put_nowait, None
            )

    def tickPrice(
        self, reqId: TickerId, tickType: TickType, price: float, attrib: TickAttrib
    ):
        security = self.mkt_data_req_id_to_security[reqId]
        if tickType == 1:  # bid
            self.quotes[security].bid = price
        elif tickType == 2:  # ask
            self.quotes[security].ask = price
        elif tickType == 4:  # last
            self.trades[security].price = price

    def tickSize(self, reqId: TickerId, tickType: TickType, size: int):
        security = self.mkt_data_req_id_to_security[reqId]
        now = pd.Timestamp.now(tz="America/New_York")
        if tickType == 0:  # bid size
            self.quotes[security].bid_size = size
            self.quotes[security].time = now
        elif tickType == 3:  # ask size
            self.quotes[security].ask_size = size
            self.quotes[security].time = now
        elif tickType == 5:  # last size
            self.trades[security].size = size
            self.trades[security].time = now
        if tickType in {0, 3} and security in self.quote_subscriptions:
            if (
                self.quotes[security].bid_size <= 0
                and self.quotes[security].ask_size <= 0
            ):
                return
            self.event_loop.call_soon_threadsafe(
                self.quote_subscriptions[security].put_nowait,
                copy.deepcopy(self.quotes[security]),
            )
        elif tickType in {5} and security in self.trade_subscriptions:
            if self.trades[security].size <= 0:
                return
            self.event_loop.call_soon_threadsafe(
                self.trade_subscriptions[security].put_nowait,
                copy.deepcopy(self.trades[security]),
            )

    def error(self, reqId: TickerId, errorCode: int, errorString: str):
        super().error(reqId, errorCode, errorString)
        # https://interactivebrokers.github.io/tws-api/message_codes.html
        if reqId in self.hist_bar_req_queue:
            if errorCode == 162:  # Historical market data Service error message.
                self.event_loop.call_soon_threadsafe(
                    self.hist_bar_req_queue[reqId].put_nowait, NotImplemented
                )
            else:
                self.event_loop.call_soon_threadsafe(
                    self.hist_bar_req_queue[reqId].put_nowait,
                    errors.DataSourceError(errorString),
                )
        if reqId in self.mkt_data_req_id_to_security:
            security = self.mkt_data_req_id_to_security[reqId]
            if security in self.quote_subscriptions:
                self.event_loop.call_soon_threadsafe(
                    self.quote_subscriptions[security].put_nowait,
                    errors.DataSourceError(errorString),
                )
            if security in self.trade_subscriptions:
                self.event_loop.call_soon_threadsafe(
                    self.trade_subscriptions[security].put_nowait,
                    errors.DataSourceError(errorString),
                )


def _time_delta_to_duration_str(time_delta: pd.Timedelta) -> str:
    """
    Converts a pandas Timedelta to an IBKR duration string. This is used by
    `get_hist_bars` to convert the request range to a duration string.

    This assume that the time_delta is a multiple of a number of days.

    https://interactivebrokers.github.io/tws-api/historical_bars.html#hd_duration

    :param time_delta: the Timedelta to convert
    :return: an IBKR compliant duration string (rounded up if necessary)
    """
    time_delta = time_delta.ceil("D")
    if time_delta < pd.Timedelta("1 day"):
        raise ValueError("Time delta has to be at least 1 day long")
    if time_delta % pd.Timedelta("1 W") == pd.Timedelta(0):
        return f"{time_delta // pd.Timedelta('1 W')} W"
    return f"{time_delta // pd.Timedelta('1 day')} D"


def _time_delta_to_bar_size_str(  # pylint: disable=too-many-return-statements
    time_delta: pd.Timedelta,
) -> Optional[str]:
    """
    Converts a pandas Timedelta to an IBKR bar size string.

    https://interactivebrokers.github.io/tws-api/historical_bars.html#hd_barsize

    :param time_delta: the Timedelta to convert
    :return: an IBKR compliant bar size (or None if the time delta is incompatible)
    """
    if time_delta < pd.Timedelta(1, unit="min"):
        count = int(time_delta.total_seconds())
        if count not in {1, 5, 10, 15, 30}:
            return None
        return f"{count} secs"
    if time_delta < pd.Timedelta(1, unit="hr"):
        count = int(time_delta.total_seconds())
        if count % 60 != 0:
            return None
        count //= 60
        if count not in {1, 2, 3, 5, 10, 15, 20, 30}:
            return None
        return f"{count} min"
    if time_delta < pd.Timedelta(1, unit="day"):
        count = int(time_delta.total_seconds())
        if count % (60 * 60) != 0:
            return None
        count //= 60 * 60
        if count not in {1, 2, 3, 4, 8}:
            return None
        unit = "hour"
        if count > 1:
            unit += "s"
        return f"{count} {unit}"
    if time_delta == pd.Timedelta(1, unit="day"):
        return "1 day"
    if time_delta == pd.Timedelta(1, unit="week"):
        return "1 week"
    if pd.Timedelta(30, unit="day") <= time_delta <= pd.Timedelta(31, unit="day"):
        return "1 month"
    return None
