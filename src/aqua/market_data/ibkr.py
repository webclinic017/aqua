"""
IBKR market data using official Python library
"""
import asyncio
import logging
import os
import threading
from typing import Any, Optional

import ibapi.client
import ibapi.contract
import ibapi.errors
import ibapi.wrapper
import pandas as pd
from dotenv import load_dotenv
from ibapi.common import BarData, TickerId

from aqua.market_data import errors, market_data_interface
from aqua.security import Option

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


class IBKRMarketData(market_data_interface.IMarketData, ibapi.wrapper.EWrapper):
    """
    IBKR market data from Interactive Brokers
    """

    def __init__(self):
        ibapi.wrapper.EWrapper.__init__(self)
        self.client = ibapi.client.EClient(self)
        self.msg_thread: Optional[threading.Thread] = None
        self.req_id = 0
        self.order_id: Optional[int] = None
        self.client_lock = threading.Lock()  # for locking req_id and order_id

        # maps request id -> (event loop, asyncio queue)
        # this is so the EWrapper method (callback) can know which
        # event loop the asyncio queue lives on (in order to put an element
        # on that queue
        self.req_queue: dict[int, tuple[Any, asyncio.Queue]] = {}

    async def __aenter__(self) -> "IBKRMarketData":
        event_loop = asyncio.get_running_loop()
        await event_loop.run_in_executor(
            None, lambda: self.client.connect(_TWS_URL, _TWS_PORT, clientId=0)
        )
        self.msg_thread = threading.Thread(
            target=self.client.run, name="ibkr_msg_thread"
        )
        self.msg_thread.start()

        async def wait_for_order():
            while self.order_id is None:
                await asyncio.sleep(0.1)

        await wait_for_order()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        event_loop = asyncio.get_running_loop()
        await event_loop.run_in_executor(None, self.client.disconnect)
        await event_loop.run_in_executor(None, self.msg_thread.join)
        if self.msg_thread.is_alive():
            logger.warning("msg thread is still running")
        self.msg_thread = None
        with self.client_lock:
            self.req_id = 0
            self.order_id = None

    async def get_option_bar_history(
        self,
        option: Option,
        start: pd.Timestamp,
        end: pd.Timestamp,
        bar_size: pd.Timedelta,
    ) -> pd.DataFrame:
        con = ibapi.contract.Contract()
        con.symbol = option.underlying.symbol
        con.secType = "OPT"
        con.lastTradeDateOrContractMonth = option.expiration.strftime("%Y%m%d")
        con.strike = option.strike
        if option.parity == Option.Parity.CALL:
            con.right = "C"
        elif option.parity == Option.Parity.PUT:
            con.right = "P"
        con.exchange = "SMART"
        con.currency = "USD"
        duration_str = _time_delta_to_duration_str(end - start)
        bar_size_str = _time_delta_to_bar_size_str(bar_size)
        with self.client_lock:
            req_id = self.req_id
            self.req_id += 1
            self.req_queue[req_id] = asyncio.get_running_loop(), asyncio.Queue()
        self.client.reqHistoricalData(
            req_id,
            con,
            end.strftime("%Y%m%d %H:%M:%S"),
            duration_str,
            bar_size_str,
            "TRADES",
            0,
            1,
            False,
            [],
        )
        bar_queue = self.req_queue[req_id][1]
        bars = list()
        bar = await bar_queue.get()
        while bar is not None:
            if isinstance(bar, Exception):
                raise bar
            if isinstance(bar, BarData):
                bars.append(
                    {
                        "Time": pd.to_datetime(bar.date, format="%Y%m%d  %H:%M:%S"),
                        "Open": bar.open,
                        "High": bar.high,
                        "Low": bar.low,
                        "Close": bar.close,
                        "Volume": bar.volume,
                        "NumTrades": bar.barCount,
                    }
                )
            else:
                logger.error("req queue received unexpected type: %s", type(bar))
                raise errors.DataSourceError
            bar = await bar_queue.get()
        del self.req_queue[req_id]
        res = pd.DataFrame(bars).set_index("Time").sort_index()
        res = res.loc[slice(start, None, None)]
        res.index = res.index.tz_localize("America/New_York")
        return res

    def historicalData(self, reqId: int, bar: BarData):
        if reqId in self.req_queue:
            loop, queue = self.req_queue[reqId]
            loop.call_soon_threadsafe(queue.put_nowait, bar)

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        if reqId in self.req_queue:
            loop, queue = self.req_queue[reqId]
            loop.call_soon_threadsafe(queue.put_nowait, None)

    def error(self, reqId: TickerId, errorCode: int, errorString: str):
        logger.info("ibkr error (%d) (%d) %s", reqId, errorCode, errorString)
        if reqId in self.req_queue:
            loop, queue = self.req_queue[reqId]
            loop.call_soon_threadsafe(
                queue.put_nowait, errors.DataSourceError(errorString)
            )
        if reqId == -1 and errorCode == ibapi.errors.CONNECT_FAIL.code():
            raise ConnectionError

    def nextValidId(self, orderId: int):
        self.order_id = orderId


def _time_delta_to_duration_str(time_delta: pd.Timedelta) -> str:
    """
    Converts a pandas Timedelta to an IBKR duration string.
    https://interactivebrokers.github.io/tws-api/historical_bars.html#hd_duration
    @return: an IBKR compliant duration string (rounded up if necessary)
    """
    seconds = int(time_delta.total_seconds())
    if seconds % (60 * 60 * 24) == 0:
        return f"{seconds // (60 * 60 * 24)} D"
    return f"{seconds} S"


def _time_delta_to_bar_size_str(  # pylint: disable=too-many-return-statements
    time_delta: pd.Timedelta,
) -> Optional[str]:
    """
    Converts a pandas Timedelta to an IBKR bar size string.
    https://interactivebrokers.github.io/tws-api/historical_bars.html#hd_barsize
    @return: an IBKR compliant bar size (or None if the time delta is incompatible)
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
