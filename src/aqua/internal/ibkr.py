"""
Implemented the IBroker interface for IBKR
"""
import asyncio
import logging
import os
import threading
from typing import Optional

import ibapi.client
import ibapi.contract
import ibapi.errors
import ibapi.wrapper
import pandas as pd
from dotenv import load_dotenv
from ibapi.common import TickerId

from aqua.market_data import errors
from aqua.security import Option, Stock, security

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


class IBKRBase(ibapi.wrapper.EWrapper):
    """
    IBKR Broker base class for setting up connections to TWS/IB Gateway
    """

    def __init__(self, client_id: Optional[int] = None):
        ibapi.wrapper.EWrapper.__init__(self)
        if client_id is None:
            client_id = 0
        self.client_id = client_id
        self.client = ibapi.client.EClient(self)
        self.msg_thread: Optional[threading.Thread] = None
        self.order_id: Optional[int] = None
        self.received_order_event = asyncio.Event()
        self.event_loop: Optional[asyncio.AbstractEventLoop] = None

    async def __aenter__(self) -> "IBKRBase":
        self.event_loop = asyncio.get_running_loop()
        await self.event_loop.run_in_executor(
            None,
            lambda: self.client.connect(_TWS_URL, _TWS_PORT, clientId=self.client_id),
        )
        self.msg_thread = threading.Thread(
            target=self.client.run, name="ibkr_msg_thread"
        )
        self.msg_thread.start()
        await self.received_order_event.wait()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.event_loop.run_in_executor(None, self.client.disconnect)
        await self.event_loop.run_in_executor(None, self.msg_thread.join)
        self.msg_thread = None
        self.order_id = None
        self.received_order_event.clear()
        self.event_loop = None

    # EWrapper methods
    def error(self, reqId: TickerId, errorCode: int, errorString: str):
        logger.info("ibkr error (%d) (%d) %s", reqId, errorCode, errorString)
        if reqId == -1 and errorCode in {
            ibapi.errors.CONNECT_FAIL.code(),
            ibapi.errors.SOCKET_EXCEPTION.code(),
        }:
            raise ConnectionError

    def nextValidId(self, orderId: int):
        self.order_id = orderId
        self.event_loop.call_soon_threadsafe(self.received_order_event.set)


def ibkr_contract_to_security(
    con: ibapi.contract.Contract,
) -> Optional[security.Security]:
    """
    Converts an ibapi Contract to an aqua Security
    @param con: ibapi Contract
    @return: an aqua Security or None if conversion can't be made
    """
    if con.secType == "STK":
        return Stock(con.symbol)
    if con.secType == "OPT":
        return Option(
            Stock(con.tradingClass),
            pd.to_datetime(con.lastTradeDateOrContractMonth, format="%Y%m%d"),
            con.strike,
            Option.Parity.CALL if con.right.lower()[0] == "c" else Option.Parity.PUT,
            Option.Type.AMERICAN,  # TODO: detect American or European
        )
    return None


def security_to_ibkr_contract(
    sec: security.Security,
) -> Optional[ibapi.contract.Contract]:
    """
    Converts an aqua Security to an ibapi Contract
    @param sec: the aqua Security to convert
    @return: an ibapi Contract or None if conversion can't be made
    """
    con = ibapi.contract.Contract()
    if isinstance(sec, Stock):
        con.secType = "STK"
        con.symbol = sec.symbol
    elif isinstance(sec, Option):
        con.secType = "OPT"
        con.symbol = sec.underlying.symbol
        con.lastTradeDateOrContractMonth = sec.expiration.strftime("%Y%m%d")
        con.strike = sec.strike
        if sec.parity == Option.Parity.CALL:
            con.right = "C"
        elif sec.parity == Option.Parity.PUT:
            con.right = "P"
        con.exchange = "SMART"
        con.currency = "USD"
    return con
