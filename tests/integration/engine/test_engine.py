import asyncio

import pytest

from aqua.engine.engine import Engine


@pytest.mark.asyncio
@pytest.mark.live
async def test_engine_run():
    engine = Engine()
    engine_task = asyncio.get_running_loop().create_task(engine.run())

    while len(engine.quotes) == 0:
        await asyncio.sleep(1)
    print(engine.quotes, engine.cash_bal)

    engine.stop()
    while not engine_task.done():
        await asyncio.sleep(0)


@pytest.mark.asyncio
@pytest.mark.live
async def test_engine_nav():
    engine = Engine()
    engine_task = asyncio.get_running_loop().create_task(engine.run())

    while (nav := engine.nav()) != float("nan"):
        await asyncio.sleep(1)
    assert nav > 0
    print("Portfolio NAV:", nav)

    engine.stop()
    while not engine_task.done():
        await asyncio.sleep(0)
