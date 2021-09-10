"""
Alpaca market data
"""
import asyncio
import json
import logging
import os
import urllib.parse
from typing import Any, Iterable, Mapping, Optional, Union

import aiohttp
import pandas as pd
import websockets.exceptions
import websockets.legacy.client
from dotenv import load_dotenv

from aqua.market_data import errors
from aqua.market_data.market_data_interface import (
    IMarketData,
    Quote,
    StreamType,
    Trade,
    _set_time,
)
from aqua.security import Stock
from aqua.security.security import Security

logger = logging.getLogger(__name__)

if not load_dotenv():
    logger.warning("Can't load dotenv file")

_ALPACA_URL = os.getenv("ALPACA_URL")
_ALPACA_DATA_URL = os.getenv("ALPACA_DATA_URL")
_ALPACA_DATA_WS_URL = os.getenv("ALPACA_DATA_WS_URL")
_ALPACA_KEY_ID = os.getenv("ALPACA_KEY_ID")
_ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")

if _ALPACA_URL is None or _ALPACA_DATA_URL is None or _ALPACA_DATA_WS_URL is None:
    logger.fatal("Unable to load Alpaca urls")
    raise errors.ConfigError

if _ALPACA_KEY_ID is None or _ALPACA_SECRET_KEY is None:
    logger.fatal("Can't load alpaca keys")
    raise errors.CredentialError


class AlpacaMarketData(IMarketData):
    """
    Alpaca market data gets market data from alpaca asynchronously
    """

    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.client: Optional[websockets.legacy.client.WebSocketClientProtocol] = None
        self.ws_task: Optional[asyncio.Task] = None
        self.trade_subscriptions: dict[Security, asyncio.Queue[Trade]] = {}
        self.quote_subscriptions: dict[Security, asyncio.Queue[Quote]] = {}

    async def __aenter__(self) -> "AlpacaMarketData":
        self.session = aiohttp.ClientSession(
            headers={
                "APCA-API-KEY-ID": _ALPACA_KEY_ID,
                "APCA-API-SECRET-KEY": _ALPACA_SECRET_KEY,
            }
        )
        await self._connect_ws()
        self.ws_task = asyncio.get_running_loop().create_task(self._ws_task())
        return self

    async def __aexit__(self, *exec_info):
        self.ws_task.cancel("socket closed")
        await self.client.close()
        self.client = None
        await self.session.close()
        await asyncio.sleep(
            0.25
        )  # https://docs.aiohttp.org/en/stable/client_advanced.html#graceful-shutdown
        self.session = None
        self.trade_subscriptions.clear()
        self.quote_subscriptions.clear()

    @property
    def name(self) -> str:
        return "Alpaca"

    async def get_hist_bars(
        self,
        security: Security,
        bar_size: pd.Timedelta,
        start_date: pd.Timestamp,
        end_date: Optional[pd.Timestamp] = None,
    ) -> Union[pd.DataFrame, type(NotImplemented)]:
        # validate security
        if not isinstance(security, Stock):
            logger.warning(
                "Polygon.io only supports stock historical bars. Got security type: %s",
                type(security),
            )
            return NotImplemented
        # validate start and end dates
        if end_date is None:
            end_date = start_date
        start_date = _set_time(start_date).floor("D")
        end_date = _set_time(end_date).floor("D")
        if end_date < start_date:
            raise ValueError("End date cannot come before start date")
        # validate bar_size and generate periods to query based on bar_size
        if bar_size.value <= 0:
            raise ValueError(f"Got non positive bar_size: {bar_size}")
        if bar_size == pd.Timedelta("1 min"):
            timeframe = "1Min"
            period_freq = pd.DateOffset(days=10)
        elif bar_size == pd.Timedelta("1 hr"):
            timeframe = "1Hour"
            period_freq = pd.DateOffset(days=600)
        elif bar_size == pd.Timedelta("1 day"):
            timeframe = "1Day"
            period_freq = pd.DateOffset(days=9000)
        else:
            logger.warning("Bar size %s not supported", bar_size)
            return NotImplemented
        periods = list(pd.date_range(start_date, end_date, freq=period_freq))
        periods.append(end_date + pd.DateOffset(days=1))
        # make requests for each period
        res = []
        for i in range(len(periods) - 1):
            period_start = periods[i]
            period_end = periods[i + 1] - pd.DateOffset(days=1)
            response = await self._get_stock_hist_bars(
                security.symbol,
                period_start.strftime("%Y-%m-%d"),
                period_end.strftime("%Y-%m-%d"),
                timeframe,
            )
            if response.empty:
                continue
            res.append(response)
        res = pd.concat(res)
        res.rename(
            columns={
                "t": "Time",
                "o": "Open",
                "h": "High",
                "l": "Low",
                "c": "Close",
                "v": "Volume",
                "n": "NumTrades",
                "vw": "VWAP",
            },
            inplace=True,
        )
        res["Time"] = res["Time"].map(lambda x: pd.Timestamp(x, tz="America/New_York"))
        res.set_index("Time", inplace=True)
        res.sort_index(inplace=True)
        return res

    async def subscribe(
        self, stream_type: StreamType, security: Security
    ) -> Union[None, type(NotImplemented)]:
        if not isinstance(security, Stock):
            logger.warning("Alpaca only supports streaming stock trades")
            return NotImplemented
        msg = {"action": "subscribe", "trades": [], "quotes": [], "bars": []}
        if stream_type == StreamType.QUOTES:
            msg["quotes"] = [security.symbol]
            subscription = self.quote_subscriptions
        elif stream_type == StreamType.TRADES:
            msg["trades"] = [security.symbol]
            subscription = self.trade_subscriptions
        else:
            logger.warning("Alpaca doesn't support stream type: %s", stream_type)
            return NotImplemented
        await self.client.send(json.dumps(msg))
        while security not in subscription:
            await asyncio.sleep(0)

    async def get(
        self, stream_type: StreamType, security: Security
    ) -> Union[Quote, Trade]:
        if stream_type == StreamType.QUOTES:
            subscription = self.quote_subscriptions
        elif stream_type == StreamType.TRADES:
            subscription = self.trade_subscriptions
        else:
            raise NotImplementedError
        if security not in subscription:
            raise ValueError(f"{security} {stream_type} not subscribed to")
        while subscription[security].empty():
            await asyncio.sleep(0)
        return await subscription[security].get()

    async def unsubscribe(self, stream_type: StreamType, security: Security) -> None:
        if not isinstance(security, Stock):
            logger.warning("Alpaca only supports streaming stock trades")
            raise NotImplementedError
        msg = {"action": "unsubscribe", "trades": [], "quotes": [], "bars": []}
        if stream_type == StreamType.QUOTES:
            msg["quotes"] = [security.symbol]
            subscription = self.quote_subscriptions
        elif stream_type == StreamType.TRADES:
            msg["trades"] = [security.symbol]
            subscription = self.trade_subscriptions
        else:
            logger.warning("Alpaca doesn't support stream type: %s", stream_type)
            raise NotImplementedError
        await self.client.send(json.dumps(msg))
        while security in subscription:
            await asyncio.sleep(0)

    # private methods

    async def _connect_ws(self):
        # connect
        url = urllib.parse.urljoin(_ALPACA_DATA_WS_URL, "/v2/iex")
        self.client = await websockets.connect(url)  # pylint: disable=no-member
        # receive and parse welcome message
        msgs = json.loads(await self.client.recv())
        if not isinstance(msgs, list) or len(msgs) != 1:
            logger.warning("Invalid handshake: %s", msgs)
            raise ConnectionError
        welcome_msg = msgs[0]
        if welcome_msg["T"] != "success" or welcome_msg["msg"] != "connected":
            logger.warning("Invalid welcome message: %s", welcome_msg)
            raise ConnectionError
        # send auth details
        await self.client.send(
            json.dumps(
                {
                    "action": "auth",
                    "key": _ALPACA_KEY_ID,
                    "secret": _ALPACA_SECRET_KEY,
                }
            )
        )
        # parse auth response
        msgs = json.loads(await self.client.recv())
        if not isinstance(msgs, list) or len(msgs) != 1:
            logger.warning("Invalid handshake: %s", msgs)
            raise ConnectionError
        auth_res = msgs[0]
        if auth_res["T"] != "success" or auth_res["msg"] != "authenticated":
            logger.warning("Unable to authenticate: %s", auth_res)
            raise ConnectionError

    async def _process_ws_msg(
        self,
    ):  # pylint: disable=too-many-branches, too-many-locals
        try:
            msgs = json.loads(await self.client.recv())
        except websockets.exceptions.ConnectionClosedOK:
            return
        except websockets.exceptions.ConnectionClosedError as closed_exception:
            raise ConnectionError("Alpaca closed connection") from closed_exception
        for msg in msgs:
            if msg["T"] == "error":
                err_code = msg["code"]
                err_msg = msg["msg"]
                if err_code == 405:
                    raise errors.DataPermissionError(err_msg)
                if err_code == 406:
                    raise errors.RateLimitError(err_msg)
                if err_code == 408:
                    raise errors.DataPermissionError(err_msg)
                if err_code == 409:
                    raise errors.DataPermissionError(err_msg)
                raise errors.DataSourceError(err_msg)
            if msg["T"] == "subscription":
                trade_subscriptions = set(Stock(symbol) for symbol in msg["trades"])
                new_trade_subscriptions = (
                    trade_subscriptions - self.trade_subscriptions.keys()
                )
                old_trade_subscriptions = (
                    self.trade_subscriptions.keys() - trade_subscriptions
                )
                for new_trade_subscription in new_trade_subscriptions:
                    self.trade_subscriptions[new_trade_subscription] = asyncio.Queue()
                for old_trade_subscription in old_trade_subscriptions:
                    del self.trade_subscriptions[old_trade_subscription]
                quote_subscriptions = set(Stock(symbol) for symbol in msg["quotes"])
                new_quote_subscriptions = (
                    quote_subscriptions - self.quote_subscriptions.keys()
                )
                old_quote_subscriptions = (
                    self.quote_subscriptions.keys() - quote_subscriptions
                )
                for new_quote_subscription in new_quote_subscriptions:
                    self.quote_subscriptions[new_quote_subscription] = asyncio.Queue()
                for old_quote_subscription in old_quote_subscriptions:
                    del self.quote_subscriptions[old_quote_subscription]
            elif msg["T"] == "t":  # trade update
                security = Stock(msg["S"])
                trade = Trade(
                    msg["p"],
                    msg["s"],
                    pd.Timestamp(msg["t"]).tz_convert("America/New_York"),
                )
                if security in self.trade_subscriptions:
                    await self.trade_subscriptions[security].put(trade)
            elif msg["T"] == "q":  # quote update
                security = Stock(msg["S"])
                quote = Quote(
                    msg["bp"],
                    msg["bs"],
                    msg["ap"],
                    msg["as"],
                    pd.Timestamp(msg["t"]).tz_convert("America/New_York"),
                )
                if security in self.quote_subscriptions:
                    await self.quote_subscriptions[security].put(quote)

    async def _ws_task(self):
        while True:
            await self._process_ws_msg()

    async def _get(
        self, path: str, params: Optional[Mapping[str, str]] = None
    ) -> Iterable[Any]:
        url = urllib.parse.urljoin(_ALPACA_DATA_URL, path)
        if params is None:
            params = {}
        params["limit"] = "10000"
        responses = []
        has_next = True
        while has_next:
            async with self.session.get(url, params=params) as response:
                if response.status != 200:
                    logger.warning(
                        "Got error %s: %s", response.status, await response.json()
                    )
                    if response.status == 429:
                        logger.warning("Rate limited")
                        raise errors.RateLimitError
                    if response.status == 422:
                        json_content = await response.json()
                        if json_content.get("code", -1) == 42210000:
                            # subscription does not permit querying data from the past 15 minutes
                            raise errors.DataPermissionError
                    raise errors.DataSourceError
                response = await response.json()
                responses.append(response)
                next_page_token = response.get("next_page_token", None)
                if next_page_token is not None:
                    params["next_page_token"] = next_page_token
                else:
                    has_next = False
        return responses

    async def _get_stock_hist_bars(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        timeframe: str,
    ) -> pd.DataFrame:
        """Assumes start_date and end_date do not need to be URL escaped"""
        responses = await self._get(
            f"/v2/stocks/{urllib.parse.quote(symbol)}/bars",
            params={"start": start_date, "end": end_date, "timeframe": timeframe},
        )
        res = []
        for response in responses:
            if "bars" in response:
                res.append(pd.DataFrame(response["bars"]))
        res = pd.concat(res)
        return res
