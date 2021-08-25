# pylint: disable=missing-function-docstring
"""Tests aqua webserver"""
import logging

import pytest
import websockets
import websockets.exceptions

from aqua.server.webserver import WebServer, WebServerCloseCode


@pytest.fixture(name="web_server")
def web_server_fixture():
    return WebServer()


@pytest.mark.asyncio
async def test_server_auth_fail(web_server: WebServer, caplog):
    caplog.set_level(logging.INFO, logger="aqua")
    web_server.run(4000)

    async def connect_client():
        async with websockets.connect(  # pylint: disable=no-member
            "ws://localhost:4000"
        ) as websocket:
            await websocket.send("notprod 5")
            response = await websocket.recv()
            assert response == "auth: failed"
            try:
                await websocket.recv()
            except websockets.exceptions.ConnectionClosedError as closed_exception:
                assert closed_exception.code == WebServerCloseCode.AUTH_FAILED.value

    await connect_client()
    web_server.stop()


@pytest.mark.asyncio
async def test_server_wrong_path(web_server: WebServer, caplog):
    caplog.set_level(logging.INFO, logger="aqua")
    web_server.run(4000)

    async def connect_client():
        async with websockets.connect(  # pylint: disable=no-member
            "ws://localhost:4000/fake_path"
        ) as websocket:
            try:
                await websocket.send("notprod")
                response = await websocket.recv()
                assert response == "auth: success"
            except websockets.exceptions.ConnectionClosedError as closed_exception:
                assert closed_exception.code == WebServerCloseCode.INVALID_PATH.value

    await connect_client()
    web_server.stop()


@pytest.mark.asyncio
async def test_server_ping(web_server: WebServer, caplog):
    caplog.set_level(logging.INFO, logger="aqua")
    web_server.run(4000)

    async def connect_client():
        async with websockets.connect(  # pylint: disable=no-member
                "ws://localhost:4000/"
        ) as websocket:
            await websocket.send("notprod")
            await websocket.recv()
            await websocket.send("5 ping")
            assert await websocket.recv() == "5 0\npong"

    await connect_client()
    web_server.stop()


@pytest.mark.asyncio
async def test_server_double_run(web_server: WebServer, caplog):
    caplog.set_level(logging.INFO, logger="aqua")
    web_server.run(4000)
    with pytest.raises(RuntimeError):
        web_server.run(4000)
    web_server.stop()
