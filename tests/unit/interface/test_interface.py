# pylint: disable=missing-module-docstring, missing-function-docstring
import pytest

from aqua.interface.interface import Interface


@pytest.fixture(name="interface")
def get_interface():
    return Interface()


@pytest.mark.asyncio
async def test_unrecognized(interface):
    async with interface:
        await interface.input("0 not_a_command")
        response = await interface.output()
        assert response.startswith("0 -1\nunrecognized request")


@pytest.mark.asyncio
async def test_ping(interface):
    async with interface:
        await interface.input("0 ping")
        response = await interface.output()
        assert response == "0 0\npong"


@pytest.mark.asyncio
async def test_sleep(interface):
    async with interface:
        await interface.input("0 sleep 1")
        await interface.input("1 ping")
        response = await interface.output()
        assert response == "1 0\npong"
        response = await interface.output()
        assert response == "0 0\nsleep done"
