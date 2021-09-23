"""
Implementation of the main engine class
"""
import asyncio
import logging
from typing import Optional

from aqua.broker import IBKRBroker
from aqua.internal.ref_counter import AsyncReferenceCounter
from aqua.market_data import MarketData
from aqua.market_data.market_data_interface import Quote, StreamType
from aqua.security.security import Security

logger = logging.getLogger(__name__)


class Engine:  # pylint: disable=too-many-instance-attributes
    """
    The engine is the main driver of aqua.

    It maintains a set of market data subscriptions and uses reference counting to keep
    subscriptions alive.
    """

    def __init__(self):
        self._market_data = MarketData()
        self._broker = IBKRBroker()

        self._positions_task: Optional[asyncio.Task] = None
        self._position_update_lock: Optional[asyncio.Lock] = None
        self.positions: dict[Security, float] = {}
        self.cash_bal: float = float("nan")

        self._quotes_tasks: dict[Security, asyncio.Task[Quote]] = {}
        self.quotes: dict[Security, Quote] = {}
        self._quotes_subscriptions: AsyncReferenceCounter[
            Security
        ] = AsyncReferenceCounter(
            init=lambda sec: self._init_subscription(StreamType.QUOTES, sec),
            deinit=lambda sec: self._deinit_subscription(StreamType.QUOTES, sec),
        )

    async def connect(self):
        """Connects market data and broker"""
        await self._market_data.__aenter__()
        await self._broker.__aenter__()

    async def disconnect(self):
        """Disconnects market data and broker"""
        await self._market_data.__aexit__(None, None, None)
        await self._broker.__aexit__(None, None, None)

    async def subscribe_positions(self):
        """Initiates a subscription to positions represented by broker"""
        await self._broker.subscribe()
        self.positions, self.cash_bal = await self._broker.get_positions()
        for sec in self.positions:
            await self._quotes_subscriptions.new(sec)
        self._position_update_lock = asyncio.Lock()

        async def update_loop():
            while True:
                prev_pos = self.positions
                new_positions, new_cash_bal = await self._broker.get_positions()
                async with self._position_update_lock:
                    self.positions, self.cash_bal = new_positions, new_cash_bal
                    await self._process_position_updates(prev_pos)

        event_loop = asyncio.get_running_loop()
        self._positions_task = event_loop.create_task(update_loop())

    async def unsubscribe_positions(self):
        """Unsubscribes from position updates"""
        if self._positions_task is None:
            return
        async with self._position_update_lock:
            self._positions_task.cancel()
        for sec in self.positions:
            await self._quotes_subscriptions.delete(sec)
        self.positions.clear()
        self.cash_bal = float("nan")
        self._positions_task = None

    async def _process_position_updates(self, prev_positions: dict[Security, float]):
        assert self.positions is not None
        new_pos = self.positions.keys() - prev_positions.keys()
        old_pos = prev_positions.keys() - self.positions.keys()
        for sec in new_pos:
            await self._quotes_subscriptions.new(sec)
        for sec in old_pos:
            await self._quotes_subscriptions.delete(sec)

    async def _init_subscription(self, stream_type: StreamType, security: Security):
        event_loop = asyncio.get_running_loop()
        await self._market_data.subscribe(stream_type, security)

        async def update_loop():
            while True:
                quote = await self._market_data.get(StreamType.QUOTES, security)
                self.quotes[security] = quote

        self._quotes_tasks[security] = event_loop.create_task(update_loop())

    async def _deinit_subscription(self, stream_type: StreamType, security: Security):
        self._quotes_tasks[security].cancel()
        del self._quotes_tasks[security]
        await self._market_data.unsubscribe(stream_type, security)
