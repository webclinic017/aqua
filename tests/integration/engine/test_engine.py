import asyncio
import math

import pytest

from aqua.engine.engine import Engine


@pytest.mark.asyncio
@pytest.mark.live
async def test_engine_get_positions():
    engine = Engine()
    await engine.connect()

    await engine.subscribe_positions()
    assert len(engine.positions) > 0
    assert not math.isnan(engine.cash_bal)
    print(engine.positions, engine.cash_bal)
    await asyncio.sleep(1)
    assert len(engine.quotes) > 0
    print(engine.quotes)
    await asyncio.sleep(1)
    print(engine.quotes)
    await engine.unsubscribe_positions()

    await engine.disconnect()
