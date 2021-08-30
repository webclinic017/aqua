# pylint: disable=missing-function-docstring, missing-module-docstring
import pandas as pd
import pytest

from aqua.broker import IBKRBroker
from aqua.broker.broker_interface import IBroker


@pytest.fixture(params=[IBKRBroker], name="broker_class")
def broker_class_fixture(request):
    return request.param


@pytest.mark.asyncio
async def test_broker_portfolio(broker_class):
    broker: IBroker = broker_class()
    async with broker:
        portfolio, update_time = await broker.get_portfolio_updates()
        assert "default" in portfolio.strategies
        assert len(portfolio.positions) > 0
        assert update_time <= pd.Timestamp.now(tz="America/New_York")
        print()
        print(portfolio)
