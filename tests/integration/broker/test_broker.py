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
        await broker.subscribe()
        positions, cash_bal = await broker.get_positions()
        assert len(positions) > 0
        for size in positions.values():
            assert size != 0
        print(positions)
        await broker.unsubscribe()
