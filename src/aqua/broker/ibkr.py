"""
Implemented the IBroker interface for IBKR
"""
import asyncio
import logging
from typing import Optional, Tuple

from ibapi.contract import Contract

from aqua.broker.broker_interface import IBroker
from aqua.internal.ibkr import IBKRBase, ibkr_contract_to_security
from aqua.security.security import Security

logger = logging.getLogger(__name__)


class IBKRBroker(IBKRBase, IBroker):
    """
    IBKR Broker
    """

    def __init__(self):
        IBKRBase.__init__(self, client_id=0)
        self.account: Optional[str] = None
        self.received_account_event: Optional[asyncio.Event] = None
        self.received_positions_event: Optional[asyncio.Event] = None
        self.positions_queue: Optional[
            asyncio.Queue[Tuple[dict[Security, float], float]]
        ] = None
        self.positions: dict[Security, float] = {}
        self.cash_bal = float("nan")

    async def __aenter__(self):
        self.received_account_event = asyncio.Event()
        await IBKRBase.__aenter__(self)
        await self.received_account_event.wait()

        self.received_positions_event = asyncio.Event()
        self.positions_queue = asyncio.Queue()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await IBKRBase.__aexit__(self, exc_type, exc_val, exc_tb)
        self.received_account_event.clear()
        self.account = None
        self.received_positions_event.clear()
        self.positions_queue = None
        self.positions.clear()
        self.cash_bal = float("nan")

    async def subscribe(self):
        self.client.reqAccountUpdates(True, self.account)

    async def get_positions(self) -> Tuple[dict[Security, float], float]:
        await self.received_positions_event.wait()
        return await self.positions_queue.get()

    async def unsubscribe(self):
        self.client.reqAccountUpdates(False, self.account)
        self.received_positions_event.clear()
        self.positions_queue = asyncio.Queue()
        self.positions.clear()
        self.cash_bal = float("nan")

    # EWrapper methods

    def updateAccountValue(self, key: str, val: str, currency: str, accountName: str):
        IBKRBase.updateAccountValue(self, key, val, currency, accountName)
        if key == "TotalCashBalance" and currency == "BASE":
            self.cash_bal = float(val)
            self.event_loop.call_soon_threadsafe(self._got_account_update)

    def updatePortfolio(  # pylint: disable=too-many-arguments
        self,
        contract: Contract,
        position: float,
        marketPrice: float,
        marketValue: float,
        averageCost: float,
        unrealizedPNL: float,
        realizedPNL: float,
        accountName: str,
    ):
        IBKRBase.updatePortfolio(
            self,
            contract,
            position,
            marketPrice,
            marketValue,
            averageCost,
            unrealizedPNL,
            realizedPNL,
            accountName,
        )
        sec = ibkr_contract_to_security(contract)
        if position == 0:
            if sec in self.positions:
                del self.positions[sec]
            return
        self.positions[sec] = position
        self.event_loop.call_soon_threadsafe(self._got_account_update)

    def accountDownloadEnd(self, accountName: str):
        IBKRBase.accountDownloadEnd(self, accountName)
        self.event_loop.call_soon_threadsafe(self.received_positions_event.set)
        self.event_loop.call_soon_threadsafe(self._got_account_update)

    def managedAccounts(self, accountsList: str):
        IBKRBase.managedAccounts(self, accountsList)
        self.account = accountsList.split(",")[0]
        self.event_loop.call_soon_threadsafe(self.received_account_event.set)

    # private method
    def _got_account_update(self):
        if not self.received_positions_event.is_set():
            return
        self.positions_queue.put_nowait((self.positions.copy(), self.cash_bal))
