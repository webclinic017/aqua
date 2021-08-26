"""
Alpaca streaming market data implementation
"""
import asyncio
import json
import logging
import os
import urllib.parse
from collections import defaultdict
from typing import Mapping, Optional, Set, Tuple, Union

import pandas as pd
import websockets
import websockets.client
import websockets.legacy.client
import websockets.exceptions
from dotenv import load_dotenv

from aqua.market_data import errors, market_data_interface
from aqua.security import Stock
from aqua.security.security import Security

logger = logging.getLogger(__name__)

if not load_dotenv():
    logger.warning("Can't load dotenv file")

_ALPACA_DATA_WS_URL = os.getenv("ALPACA_DATA_WS_URL")
_ALPACA_KEY_ID = os.getenv("ALPACA_KEY_ID")
_ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")

if _ALPACA_DATA_WS_URL is None:
    logger.fatal("Unable to load Alpaca WebSocket URL")
    raise errors.ConfigError

if _ALPACA_KEY_ID is None or _ALPACA_SECRET_KEY is None:
    logger.fatal("Can't load alpaca keys")
    raise errors.CredentialError

TradeUpdateMsg = Tuple[float, int, pd.Timestamp]
QuoteUpdateMsg = Tuple[Tuple[float, int], Tuple[float, int]]


class AlpacaStreamingMarketData(market_data_interface.IStreamingMarketData):
    """
    Alpaca streaming market data implementation
    """

    def __init__(self):
        self._client: Optional[websockets.legacy.client.WebSocketClientProtocol] = None
        # trades
        self._trade_subscriptions: Optional[Set[Security]] = None
        self._trade_subscription_locks: Optional[Mapping[Security, asyncio.Lock]] = None
        self._trade_msgs: Optional[
            defaultdict[Security, asyncio.Queue[TradeUpdateMsg]]
        ] = None
        # quotes
        self._quote_subscriptions: Optional[Set[Security]] = None
        self._quote_subscription_locks: Optional[Mapping[Security, asyncio.Lock]] = None
        self._quote_msgs: Optional[defaultdict[Security, asyncio.Queue]] = None

    async def __aenter__(self) -> "AlpacaStreamingMarketData":
        # connect
        url = urllib.parse.urljoin(_ALPACA_DATA_WS_URL, "/v2/iex")
        self._client = await websockets.connect(url)  # pylint: disable=no-member
        # receive and parse welcome message
        msgs = json.loads(await self._client.recv())
        if not isinstance(msgs, list) or len(msgs) != 1:
            logger.warning("Invalid handshake: %s", msgs)
            raise ConnectionError
        welcome_msg = msgs[0]
        if welcome_msg["T"] != "success" or welcome_msg["msg"] != "connected":
            logger.warning("Invalid welcome message: %s", welcome_msg)
            raise ConnectionError
        # send auth details
        await self._client.send(
            json.dumps(
                {
                    "action": "auth",
                    "key": _ALPACA_KEY_ID,
                    "secret": _ALPACA_SECRET_KEY,
                }
            )
        )
        # parse auth response
        msgs = json.loads(await self._client.recv())
        if not isinstance(msgs, list) or len(msgs) != 1:
            logger.warning("Invalid handshake: %s", msgs)
            raise ConnectionError
        auth_res = msgs[0]
        if auth_res["T"] != "success" or auth_res["msg"] != "authenticated":
            logger.warning("Unable to authenticate: %s", auth_res)
            raise ConnectionError
        # trades
        self._trade_subscriptions = set()
        self._trade_subscription_locks = defaultdict(asyncio.Lock)
        self._trade_msgs = defaultdict(asyncio.Queue)
        # quotes
        self._quote_subscriptions = set()
        self._quote_subscription_locks = defaultdict(asyncio.Lock)
        self._quote_msgs = defaultdict(asyncio.Queue)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._client.close()
        self._client = None
        # trades
        self._trade_subscriptions = None
        self._trade_subscription_locks = defaultdict(asyncio.Lock)
        self._trade_msgs = None

    async def _process_msg(self):
        try:
            msgs = json.loads(await self._client.recv())
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
                self._trade_subscriptions.clear()
                for ticker in msg["trades"]:
                    self._trade_subscriptions.add(Stock(ticker))
                self._quote_subscriptions.clear()
                for ticker in msg["quotes"]:
                    self._quote_subscriptions.add(Stock(ticker))
            elif msg["T"] == "t":  # trade update
                self._trade_msgs[Stock(msg["symbol"])].put_nowait(
                    (
                        msg["p"],
                        msg["s"],
                        pd.Timestamp(msg["t"]).tz_convert("America/New_York"),
                    )
                )

    async def _subscribe(
        self,
        security: Security,
        subscription: Set[Security],
        subscription_locks: Mapping[Security, asyncio.Lock],
        subscribe_field: str,
    ) -> Union[None, type(NotImplemented)]:
        if not isinstance(security, Stock):
            logger.warning("Alpaca only supports streaming stock trades")
            return NotImplemented
        async with subscription_locks[security]:
            msg = {
                "action": "subscribe",
                "trades": [],
                "quotes": [],
                "bars": [],
                subscribe_field: [security.symbol],
            }
            await self._client.send(json.dumps(msg))
            while security not in subscription:
                await self._process_msg()
        return None

    async def _unsubscribe(
        self,
        security: Security,
        subscriptions: Set[Security],
        subscription_locks: Mapping[Security, asyncio.Lock],
        subscribe_field: str,
        msg_queue: defaultdict[Security, asyncio.Queue],
    ):
        if not isinstance(security, Stock):
            logger.warning("Alpaca only supports streaming stock trades")
            return NotImplemented
        async with subscription_locks[security]:
            msg = {
                "action": "unsubscribe",
                "trades": [],
                "quotes": [],
                "bars": [],
                subscribe_field: [security.symbol],
            }
            await self._client.send(json.dumps(msg))
            while security in subscriptions:
                await self._process_msg()
            del msg_queue[security]

    async def _get_update(
        self,
        security: Security,
        subscriptions: Set[Security],
        subscription_locks: Mapping[Security, asyncio.Lock],
        msg_queue: Mapping[Security, asyncio.Queue],
    ):
        async with subscription_locks[security]:
            if security not in subscriptions:
                raise ValueError("Security not subscribed to trades")
        while msg_queue[security].empty():
            await self._process_msg()
        return await msg_queue[security].get()

    async def subscribe_trades(
        self, security: Security
    ) -> Union[None, type(NotImplemented)]:
        return await self._subscribe(
            security,
            self._trade_subscriptions,
            self._trade_subscription_locks,
            "trades",
        )

    async def unsubscribe_trades(
        self, security: Security
    ) -> Union[None, type(NotImplemented)]:
        return await self._unsubscribe(
            security,
            self._trade_subscriptions,
            self._trade_subscription_locks,
            "trades",
            self._trade_msgs,
        )

    async def get_trade(self, security: Security) -> TradeUpdateMsg:
        return await self._get_update(
            security,
            self._trade_subscriptions,
            self._trade_subscription_locks,
            self._trade_msgs,
        )

    @property
    async def trade_subscriptions(self) -> Set[Security]:
        return self._trade_subscriptions

    async def subscribe_quotes(
        self, security: Security
    ) -> Union[None, type(NotImplemented)]:
        return await self._subscribe(
            security,
            self._quote_subscriptions,
            self._quote_subscription_locks,
            "quotes",
        )

    async def unsubscribe_quotes(
        self, security: Security
    ) -> Union[None, type(NotImplemented)]:
        return await self._unsubscribe(
            security,
            self._quote_subscriptions,
            self._quote_subscription_locks,
            "quotes",
            self._quote_msgs,
        )

    async def get_quote(self, security: Security) -> QuoteUpdateMsg:
        return await self._get_update(
            security,
            self._quote_subscriptions,
            self._quote_subscription_locks,
            self._quote_msgs,
        )

    @property
    def quote_subscriptions(self) -> Set[Security]:
        return self._quote_subscriptions
