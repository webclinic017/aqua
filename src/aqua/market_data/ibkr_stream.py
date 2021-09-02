"""
IBKR streaming market data implementation
"""
import asyncio
import logging
from collections import defaultdict
from typing import Set, Tuple, Union

import pandas as pd
from ibapi.common import TickAttrib, TickerId
from ibapi.ticktype import TickType

from aqua.internal.ibkr import IBKRBase, security_to_ibkr_contract
from aqua.market_data import market_data_interface
from aqua.security.security import Security

logger = logging.getLogger(__name__)

TradeType = Tuple[float, int, pd.Timestamp]
QuoteType = Tuple[Tuple[float, int], Tuple[float, int], pd.Timestamp]


class IBKRStreamingMarketData(
    IBKRBase, market_data_interface.IStreamingMarketData
):  # pylint: disable=too-many-instance-attributes
    """
    Streaming market data for IBKR
    """

    def __init__(self):
        IBKRBase.__init__(self, client_id=2)
        self.req_id = 1
        self._subscriptions: defaultdict[Security, Set[int]] = defaultdict(set)
        self._request_mapping: dict[int, Security] = {}

        self._trades: defaultdict[Security, asyncio.Queue] = defaultdict(asyncio.Queue)
        self._trade_subscriptions: Set[Security] = set()
        self._trade_states: dict[Security, TradeType] = {}

        self._quotes: defaultdict[Security, asyncio.Queue] = defaultdict(asyncio.Queue)
        self._quote_subscriptions: Set[Security] = set()
        self._quote_states: dict[Security, QuoteType] = {}

    async def __aenter__(self) -> market_data_interface.IStreamingMarketData:
        await super().__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await super().__aexit__(exc_type, exc_val, exc_tb)
        self.req_id = 1
        self._subscriptions.clear()
        self._request_mapping.clear()

        self._trades.clear()
        self._trade_subscriptions.clear()
        self._trade_states.clear()

        self._quotes.clear()
        self._quote_subscriptions.clear()
        self._quote_states.clear()

    async def subscribe_trades(
        self, security: Security
    ) -> Union[None, type(NotImplemented)]:
        return await self._subscribe(
            security, self._trade_subscriptions, self._quote_subscriptions
        )

    async def unsubscribe_trades(
        self, security: Security
    ) -> Union[None, type(NotImplemented)]:
        return await self._unsubscribe(
            security, self._quote_subscriptions, self._trade_subscriptions
        )

    async def get_trade(self, security: Security) -> TradeType:
        if security not in self._trade_subscriptions:
            raise ValueError(f"{security} trades never subscribed to")
        return await self._trades[security].get()

    @property
    async def trade_subscriptions(self) -> Set[Security]:
        return self._trade_subscriptions

    async def subscribe_quotes(
        self, security: Security
    ) -> Union[None, type(NotImplemented)]:
        return await self._subscribe(
            security, self._quote_subscriptions, self._trade_subscriptions
        )

    async def unsubscribe_quotes(
        self, security: Security
    ) -> Union[None, type(NotImplemented)]:
        return await self._unsubscribe(
            security, self._quote_subscriptions, self._trade_subscriptions
        )

    async def get_quote(self, security: Security) -> QuoteType:
        if security not in self._quote_subscriptions:
            raise ValueError(f"{security} quotes were never subscribed to")
        return await self._quotes[security].get()

    @property
    async def quote_subscriptions(self) -> Set[Security]:
        return self._quote_subscriptions

    # EWrapper methods

    def tickPrice(
        self, reqId: TickerId, tickType: TickType, price: float, attrib: TickAttrib
    ):
        sec = self._request_mapping[reqId]
        if sec in self._quote_subscriptions:
            (bid, bid_size), (ask, ask_size), quote_time = self._quote_states.get(
                sec, ((None, None), (None, None), None)
            )
            if tickType == 1:  # bid price
                bid = price
                quote_time = pd.Timestamp.now(tz="America/New_York")
            elif tickType == 2:  # ask price
                ask = price
                quote_time = pd.Timestamp.now(tz="America/New_York")
            self._quote_states[sec] = (bid, bid_size), (ask, ask_size), quote_time
        if sec in self._trade_subscriptions:
            trade_price, trade_size, trade_time = self._trade_states.get(
                sec, (None, None, None)
            )
            if tickType == 4:  # last trade price
                trade_price = price
                trade_time = pd.Timestamp.now(tz="America/New_York")
            self._trade_states[sec] = trade_price, trade_size, trade_time

    def tickSize(self, reqId: TickerId, tickType: TickType, size: int):
        # tick size is always called for price and size updates
        # it's called after each tickPrice call
        sec = self._request_mapping[reqId]
        if sec in self._quote_subscriptions:
            (bid, bid_size), (ask, ask_size), quote_time = self._quote_states.get(
                sec, ((None, None), (None, None), None)
            )
            if tickType == 0:  # bid size
                bid_size = size
                quote_time = pd.Timestamp.now(tz="America/New_York")
            elif tickType == 3:  # ask size
                ask_size = size
                quote_time = pd.Timestamp.now(tz="America/New_York")
            self._quote_states[sec] = (bid, bid_size), (ask, ask_size), quote_time
            if not any(x is None for x in (bid, bid_size, ask, ask_size, quote_time)):
                if (bid, bid_size) != (-1, 0) or (ask, ask_size) != (-1, 0):
                    self.event_loop.call_soon_threadsafe(
                        self._quotes[sec].put_nowait,
                        ((bid, bid_size), (ask, ask_size), quote_time),
                    )
        if sec in self._trade_subscriptions:
            trade_price, trade_size, trade_time = self._trade_states.get(
                sec, (None, None, None)
            )
            if tickType == 5:  # last trade size
                trade_size = size
                trade_time = pd.Timestamp.now(tz="America/New_York")
            self._trade_states[sec] = trade_price, trade_size, trade_time
            if not any(x is None for x in (trade_price, trade_size, trade_time)):
                self.event_loop.call_soon_threadsafe(
                    self._trades[sec].put_nowait, (trade_price, trade_size, trade_time)
                )

    # private methods

    async def _subscribe(
        self,
        security: Security,
        subscriptions: Set[Security],
        other_subscriptions: Set[Security],
    ):
        if security in subscriptions:
            logger.warning("%s trades already subscribed", security)
            return None
        subscriptions.add(security)
        if security not in other_subscriptions:
            return await self._req_mkt_data(security)
        return None

    async def _unsubscribe(
        self,
        security: Security,
        subscriptions: Set[Security],
        other_subscriptions: Set[Security],
    ):
        if security not in subscriptions:
            logger.warning("%s was never subscribed to - can't unsubscribe", security)
            return None
        subscriptions.remove(security)
        if security not in other_subscriptions:
            return await self._cancel_mkt_data(security)
        return None

    async def _req_mkt_data(
        self, security: Security
    ) -> Union[None, type(NotImplemented)]:
        con = security_to_ibkr_contract(security)
        if con is None:
            logger.warning("IBKR doesn't support security type: %s", type(security))
            return NotImplemented
        self.client.reqMktData(self.req_id, con, "", False, False, [])
        self._subscriptions[security].add(self.req_id)
        self._request_mapping[self.req_id] = security
        self.req_id += 1
        return None

    async def _cancel_mkt_data(
        self, security: Security
    ) -> Union[None, type(NotImplemented)]:
        for req_id in self._subscriptions[security]:
            self.client.cancelMktData(req_id)
            del self._request_mapping[req_id]
        del self._subscriptions[security]
        return None
