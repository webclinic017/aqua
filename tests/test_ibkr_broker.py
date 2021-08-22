# pylint: disable=missing-module-docstring, missing-function-docstring
import pandas as pd
import pytest

from aqua.broker.ibkr import IBKRBroker


@pytest.mark.asyncio
@pytest.mark.ibkr
async def test_broker():
    async with IBKRBroker() as broker:
        now = pd.Timestamp.now()
        updates = broker.get_portfolio_updates()
        portfolio, update_time = await updates.get()
        assert update_time >= now
        assert "default" in portfolio.strategies
        assert len(portfolio["default"].positions) > 0
