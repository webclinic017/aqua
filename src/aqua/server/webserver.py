"""
webserver.py provides the main method `run` that starts the webserver
"""
import asyncio
import contextlib
import enum
import logging
import os
import secrets
import threading
from typing import Optional

import websockets
import websockets.exceptions
import websockets.legacy.server
from dotenv import load_dotenv

from aqua.interface import Interface

logger = logging.getLogger(__name__)

if not load_dotenv():
    logger.warning("Couldn't load .env file")

WebSocketType = websockets.legacy.server.WebSocketServerProtocol


class WebServerCloseCode(enum.Enum):
    """WebSocket close codes"""

    AUTH_FAILED = 3401
    INVALID_PATH = 3404


class WebServer:  # pylint: disable=too-many-instance-attributes
    """WebServer"""

    def __init__(self):
        self.server_thread: Optional[threading.Thread] = None
        self.server_started: Optional[threading.Condition] = None
        self.server: Optional[websockets.legacy.server.Serve] = None
        self.event_loop: Optional[asyncio.AbstractEventLoop] = None
        self.input_task: Optional[asyncio.Task] = None
        self.output_task: Optional[asyncio.Task] = None
        self.num_clients = 0
        self.password: Optional[str] = os.getenv("AQUA_WEBSERVER_KEY")
        assert self.password is not None, "Can't find webserver key"

    def run(self, port: int):
        """
        Starts the webserver on a given port

        :param port: port to run webserver
        """
        if self.server_thread is not None and self.server_thread.is_alive():
            raise RuntimeError("server is already running")
        self.server_started = threading.Condition()
        self.server_thread = threading.Thread(
            target=self._start_server, name="aqua server", kwargs={"port": port}
        )
        self.server_thread.start()
        with self.server_started:
            self.server_started.wait()

    def stop(self):
        """Stops the webserver"""
        self.event_loop.call_soon_threadsafe(self.server.ws_server.close)
        wait_closed = self.event_loop.create_task(self.server.ws_server.wait_closed())
        while not wait_closed.done():
            pass
        remaining_tasks = asyncio.all_tasks(self.event_loop)
        for remaining_task in remaining_tasks:
            remaining_task.cancel("server shutting down")
        self.event_loop.call_soon_threadsafe(self.event_loop.stop)
        self.server_thread.join()
        self.server_thread = None

    def _start_server(self, port: int):
        self.event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.event_loop)
        self.server = websockets.serve(  # pylint: disable=no-member
            self._on_connect, "localhost", port
        )
        try:
            logger.info("starting server on port: %d", port)
            self.event_loop.run_until_complete(self.server)
            with self.server_started:
                self.server_started.notify_all()
            self.event_loop.run_forever()
        except Exception as exception:  # pylint: disable=broad-except
            logger.warning("server thread encountered exception: %s", exception)
        logger.info("server shutting down")

    async def _on_connect(self, websocket: WebSocketType, path: str):
        """Called when a client connects"""

        def dec_num_clients():
            self.num_clients -= 1

        with contextlib.ExitStack() as exit_stack:
            self.num_clients += 1
            exit_stack.callback(dec_num_clients)
            client_id = self.num_clients
            logger.info("client %d connected", client_id)

            # check path
            if path != "/":
                logger.info("client %d wrong path", client_id)
                await websocket.close(
                    code=WebServerCloseCode.INVALID_PATH.value, reason="path not found"
                )
                return
            # auth
            if not await self._auth(websocket):
                logger.info("client %d failed auth", client_id)
                await websocket.close(
                    code=WebServerCloseCode.AUTH_FAILED.value, reason="not authorized"
                )
                return

            async with Interface() as interface:
                self.input_task = asyncio.Task(websocket.recv())
                self.output_task = asyncio.Task(interface.output())
                with contextlib.ExitStack() as task_exit_stack:
                    task_exit_stack.callback(self.input_task.cancel)
                    task_exit_stack.callback(self.output_task.cancel)
                    while True:
                        if self.input_task.done():
                            try:
                                msg = self.input_task.result()
                            except websockets.exceptions.ConnectionClosed:
                                return
                            self.input_task = asyncio.Task(websocket.recv())
                            logger.info(
                                'client %d> "%s"', client_id, msg.replace("\n", "\\n")
                            )
                            await interface.input(msg)

                        if self.output_task.done():
                            output = self.output_task.result()
                            self.output_task = asyncio.Task(interface.output())
                            logger.info(
                                'client %d< "%s"',
                                client_id,
                                output.replace("\n", "\\n"),
                            )
                            await websocket.send(output)

                        await asyncio.sleep(0)

    async def _auth(self, websocket: WebSocketType) -> bool:
        """Tries to authenticate and returns true if success, false otherwise"""
        auth_key = await websocket.recv()
        if not secrets.compare_digest(auth_key, self.password):
            await websocket.send("auth: failed")
            return False
        await websocket.send("auth: success")
        return True
