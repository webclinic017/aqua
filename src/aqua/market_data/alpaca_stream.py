"""
Alpaca streaming market data implementation
"""
import asyncio
import json
import logging
import os
import threading
import urllib.parse
from typing import Callable, Optional, Set, Union

import pandas as pd
import websockets
import websockets.client
import websockets.legacy.client
from dotenv import load_dotenv

from aqua.market_data import errors, market_data_interface
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


# TODO: implement message thread/callback
class AlpacaStreamingMarketData(market_data_interface.IStreamingMarketData):
    """
    Alpaca streaming market data implementation
    """

    def __init__(self):
        self._client: Optional[websockets.legacy.client.WebSocketClientProtocol] = None
        self._client_lock = threading.Lock()
        self._event_loop: Optional[asyncio.AbstractEventLoop] = None

        self._msg_thread: Optional[threading.Thread] = None
        self._msg_event_loop: Optional[asyncio.AbstractEventLoop] = None
        self._authenticated_event = asyncio.Event()

        self._trade_subscriptions: Set[Security] = set()

    async def __aenter__(self) -> "AlpacaStreamingMarketData":
        self._event_loop = asyncio.get_running_loop()
        self._msg_thread = threading.Thread(
            target=self._process_msgs, name="alpaca_market_data_stream"
        )
        self._msg_thread.start()
        await self._authenticated_event.wait()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        with self._client_lock:
            await self._client.close()
            self._client = None
        self._msg_thread.join()
        self._msg_thread = None
        self._msg_event_loop = None
        self._authenticated_event.clear()
        self._event_loop = None

    def _process_msgs(self):
        # running in alpaca_market_data_stream thread
        # create a new event loop for this thread
        self._msg_event_loop = asyncio.get_event_loop()
        # connect
        url = urllib.parse.urljoin(_ALPACA_DATA_WS_URL, "/v2/iex")
        self._client = self._msg_event_loop.run_until_complete(
            websockets.connect(url)  # pylint: disable=no-member
        )
        # receive and parse welcome message
        msgs = json.loads(self._msg_event_loop.run_until_complete(self._client.recv()))
        if len(msgs) != 0:
            logger.warning("Invalid handshake: %s", msgs)
            raise ConnectionError
        welcome_msg = msgs[0]
        if welcome_msg["T"] != "success" or welcome_msg["msg"] != "connected":
            logger.warning("Invalid welcome message: %s", welcome_msg)
            raise ConnectionError
        # send auth details
        self._msg_event_loop.run_until_complete(
            self._client.send(
                json.dumps(
                    {
                        "action": "auth",
                        "key": _ALPACA_KEY_ID,
                        "secret": _ALPACA_SECRET_KEY,
                    }
                )
            )
        )
        # parse auth response
        msgs = json.loads(self._msg_event_loop.run_until_complete(self._client.recv()))
        if len(msgs) != 0:
            logger.warning("Invalid handshake: %s", msgs)
            raise ConnectionError
        auth_res = msgs[0]
        if auth_res["T"] != "success" or auth_res["msg"] != "authenticated":
            logger.warning("Unable to authenticate: %s", auth_res)
            raise ConnectionError
        # authenticated
        self._event_loop.call_soon_threadsafe(self._authenticated_event.set)

    async def subscribe_trades(
        self, security: Security, callback: Callable[[float, int, pd.Timestamp], None]
    ) -> Union[bool, type(NotImplemented)]:
        pass

    async def unsubscribe_trades(
        self, security: Security
    ) -> Union[None, type(NotImplemented)]:
        pass

    @property
    async def trade_subscriptions(self) -> Set[Security]:
        return self._trade_subscriptions
