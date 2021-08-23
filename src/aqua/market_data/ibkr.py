"""
IBKR market data using official Python library
"""
import asyncio
import logging
import os
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from ibapi.common import BarData, TickerId

from aqua.internal.ibkr import IBKRBase, security_to_ibkr_contract
from aqua.market_data import errors, market_data_interface
from aqua.security import Option, Stock
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
        super().__init__()
        self.req_id = 0
        self.req_queue: dict[int, asyncio.Queue] = {}

    async def get_stock_bar_history(
        self,
        stock: Stock,
        start: pd.Timestamp,
        end: pd.Timestamp,
        bar_size: pd.Timedelta,
    ) -> pd.DataFrame:
        res = await self._get_contract_bar_history(stock, start, end, bar_size)
        res["Volume"] *= 100
        return res

    async def get_option_bar_history(
        self,
        option: Option,
        start: pd.Timestamp,
        end: pd.Timestamp,
        bar_size: pd.Timedelta,
    ) -> pd.DataFrame:
        return await self._get_contract_bar_history(option, start, end, bar_size)

    async def _get_contract_bar_history(
        self,
        security: Security,
        start: pd.Timestamp,
        end: pd.Timestamp,
        bar_size: pd.Timedelta,
    ) -> pd.DataFrame:
        con = security_to_ibkr_contract(security)
        duration_str = _time_delta_to_duration_str(end - start)
        bar_size_str = _time_delta_to_bar_size_str(bar_size)
        self.req_id += 1
        self.req_queue[self.req_id] = asyncio.Queue()
        self.client.reqHistoricalData(
            self.req_id,
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
        bar_queue = self.req_queue[self.req_id]
        bars = list()
        while (bar := await bar_queue.get()) is not None:
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
        del self.req_queue[self.req_id]
        res = pd.DataFrame(bars)
        try:
            res["Time"] = pd.to_datetime(res["Time"], format="%Y%m%d  %H:%M:%S")
        except ValueError:
            res["Time"] = pd.to_datetime(res["Time"], format="%Y%m%d")
        res = res.set_index("Time").sort_index()
        res = res.loc[slice(start, None, None)]
        res.index = res.index.tz_localize("America/New_York")
        return res

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
        if reqId in self.req_queue:
            self.event_loop.call_soon_threadsafe(
                self.req_queue[reqId].put_nowait, errors.DataSourceError(errorString)
            )


def _time_delta_to_duration_str(time_delta: pd.Timedelta) -> str:
    """
    Converts a pandas Timedelta to an IBKR duration string.

    https://interactivebrokers.github.io/tws-api/historical_bars.html#hd_duration

    :param time_delta: the Timedelta to convert
    :return: an IBKR compliant duration string (rounded up if necessary)
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
