"""
IBKR market data using official Python library
"""
import asyncio
import logging
import os
from contextlib import ExitStack
from typing import Optional, Union

import pandas as pd
from dotenv import load_dotenv
from ibapi.common import BarData, TickerId

from aqua.internal.ibkr import IBKRBase, security_to_ibkr_contract
from aqua.market_data import errors, market_data_interface
from aqua.market_data.ibkr_stream import IBKRStreamingMarketData
from aqua.market_data.market_data_interface import _set_time
from aqua.security.security import Security

logger = logging.getLogger(__name__)

if not load_dotenv():
    logger.warning("Can't load dotenv file")

_TWS_URL = os.getenv("TWS_URL")
try:
    _TWS_PORT = int(os.getenv("TWS_PORT"))
except ValueError:
    _TWS_PORT = None

if _TWS_URL is None or _TWS_PORT is None:
    logger.fatal("Unable to acquire TWS url and port")
    raise errors.ConfigError


class IBKRMarketData(IBKRBase, market_data_interface.IMarketData):
    """
    IBKR market data from Interactive Brokers
    """

    def __init__(self):
        IBKRBase.__init__(self, client_id=1)
        self.req_id = 0
        self.req_queue: dict[int, asyncio.Queue] = {}

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
        logger.info(
            "Request historical data: %s %s %s %s",
            con,
            end_date.strftime("%Y%m%d %H:%M:%S"),
            duration_str,
            bar_size_str,
        )
        self.req_id += 1
        self.req_queue[self.req_id] = asyncio.Queue()
        self.client.reqHistoricalData(
            reqId=self.req_id,
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
        bars = []
        bar_queue = self.req_queue[self.req_id]
        with ExitStack() as exit_stack:
            exit_stack.callback(self.req_queue.__delitem__, self.req_id)
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

    def get_streaming_market_data(  # pylint: disable=no-self-use
        self,
    ) -> Union[market_data_interface.IStreamingMarketData, type(NotImplemented)]:
        return IBKRStreamingMarketData()

    # EWrapper methods

    def historicalData(self, reqId: int, bar: BarData):
        super().historicalData(reqId, bar)
        if reqId in self.req_queue:
            self.event_loop.call_soon_threadsafe(self.req_queue[reqId].put_nowait, bar)

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        super().historicalDataEnd(reqId, start, end)
        if reqId in self.req_queue:
            self.event_loop.call_soon_threadsafe(self.req_queue[reqId].put_nowait, None)

    def error(self, reqId: TickerId, errorCode: int, errorString: str):
        super().error(reqId, errorCode, errorString)
        # https://interactivebrokers.github.io/tws-api/message_codes.html
        if reqId in self.req_queue:
            if errorCode == 162:  # Historical market data Service error message.
                self.event_loop.call_soon_threadsafe(
                    self.req_queue[reqId].put_nowait, NotImplemented
                )
            else:
                self.event_loop.call_soon_threadsafe(
                    self.req_queue[reqId].put_nowait,
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
