"""
Implementation of the main engine class
"""
import asyncio
import contextlib
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
        self._running = False
        self._market_data = MarketData()
        self._broker = IBKRBroker()

        self._positions_task: Optional[asyncio.Task] = None
        self.positions: dict[Security, float] = {}
        self.cash_bal: float = 0

        self._quotes_tasks: dict[Security, asyncio.Task[Quote]] = {}
        self.quotes: dict[Security, Quote] = {}
        self._quotes_subscriptions: AsyncReferenceCounter[
            Security
        ] = AsyncReferenceCounter(
            init=lambda sec: self._init_subscription(StreamType.QUOTES, sec),
            deinit=lambda sec: self._deinit_subscription(StreamType.QUOTES, sec),
        )

    async def run(self):
        """Starts running the engine, which fetches positions and quotes"""
        logger.info("starting engine")
        event_loop = asyncio.get_running_loop()
        async with contextlib.AsyncExitStack() as exit_stack:
            logger.info("establishing market data connection")
            await exit_stack.enter_async_context(self._market_data)
            logger.info("establishing broker connection")
            await exit_stack.enter_async_context(self._broker)

            async def cancel_tasks():
                if self._positions_task is not None:
                    self._positions_task.cancel()
                for task in self._quotes_tasks.values():
                    task.cancel()

            exit_stack.push_async_callback(cancel_tasks)

            await self._broker.subscribe()
            print("getting positions")
            self.positions, self.cash_bal = await self._broker.get_positions()
            await self._process_position_updates({})
            print(f"got positions: {self.positions}")
            self._positions_task = event_loop.create_task(self._broker.get_positions())
            self._running = True
            print("engine running")
            try:
                while self._running:
                    # update portfolio
                    if self._positions_task.done():
                        prev_pos = self.positions
                        self.positions, self.cash_bal = self._positions_task.result()
                        self._positions_task = event_loop.create_task(
                            self._broker.get_positions()
                        )
                        await self._process_position_updates(prev_pos)

                    # update quotes
                    quote_securities = list(self._quotes_tasks.keys())
                    for security in quote_securities:
                        if self._quotes_tasks[security].done():
                            self.quotes[security] = self._quotes_tasks[security].result()
                            self._quotes_tasks[security] = event_loop.create_task(
                                self._market_data.get(StreamType.QUOTES, security)
                            )

                    await asyncio.sleep(0)
            except Exception as exception:
                print(f"engine got exception: {exception}")

    def stop(self):
        """Stops the engine from running"""
        self._running = False

    def nav(self) -> float:
        """
        Returns the net asset value of the portfolio given its securities, cash balance,
        and mid market prices for the quotes.
        """
        nav = self.cash_bal
        for sec, size in self.positions.items():
            if sec not in self.quotes:
                return float("nan")
            nav += self.quotes[sec].mid_price() * size
        return nav

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
        self._quotes_tasks[security] = event_loop.create_task(
            self._market_data.get(StreamType.QUOTES, security)
        )

    async def _deinit_subscription(self, stream_type: StreamType, security: Security):
        self._quotes_tasks[security].cancel()
        del self._quotes_tasks[security]
        await self._market_data.unsubscribe(stream_type, security)
