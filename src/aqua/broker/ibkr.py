"""
Implemented the IBroker interface for IBKR
"""
import asyncio
import copy
import logging
import os
from typing import Optional, Tuple

import pandas as pd
from dotenv import load_dotenv
from ibapi.contract import Contract

from aqua.broker.broker_interface import IBroker
from aqua.internal.ibkr import IBKRBase, ibkr_contract_to_security
from aqua.market_data import errors
from aqua.portfolio import Portfolio, Strategy

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


class IBKRBroker(IBroker, IBKRBase):
    """
    IBKR Broker
    """

    def __init__(self):
        super().__init__()
        self.account: Optional[str] = None
        self.received_account_event = asyncio.Event()

        self.portfolio: Optional[Portfolio] = None
        self.portfolio_updates: Optional[asyncio.Queue] = None
        self.portfolio_updates_done = False
        self.portfolio_updating = False

    async def __aenter__(self):
        await IBKRBase.__aenter__(self)
        self.portfolio_updates = asyncio.Queue()
        self.portfolio_updating = False
        await self.received_account_event.wait()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await IBKRBase.__aexit__(self, exc_type, exc_val, exc_tb)
        self.portfolio_updates = None
        self.portfolio_updating = False
        self.received_account_event.clear()
        self.account = None

    async def get_portfolio_updates(self) -> Tuple[Portfolio, pd.Timestamp]:
        self.portfolio_updates_done = False
        self.portfolio = Portfolio([Strategy()])
        if not self.portfolio_updating:
            self.client.reqAccountUpdates(True, self.account)
            self.portfolio_updating = True
        return await self.portfolio_updates.get()

    def _got_portfolio_update(self):
        if self.portfolio_updates_done:
            self.event_loop.call_soon_threadsafe(
                self.portfolio_updates.put_nowait,
                (
                    copy.deepcopy(self.portfolio),
                    pd.Timestamp.now(tz="America/New_York"),
                ),
            )

    # EWrapper methods

    def updateAccountValue(self, key: str, val: str, currency: str, accountName: str):
        IBKRBase.updateAccountValue(self, key, val, currency, accountName)
        if key == "TotalCashBalance" and currency == "BASE":
            self.portfolio.cash = float(val)
        self._got_portfolio_update()

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
        self.portfolio["default"][sec] = position
        self._got_portfolio_update()

    def accountDownloadEnd(self, accountName: str):
        IBKRBase.accountDownloadEnd(self, accountName)
        self.portfolio_updates_done = True
        self._got_portfolio_update()

    def managedAccounts(self, accountsList: str):
        IBKRBase.managedAccounts(self, accountsList)
        self.account = accountsList.split(",")[0]
        self.event_loop.call_soon_threadsafe(self.received_account_event.set)
