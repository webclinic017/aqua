"""
The Interface implementation
"""
import asyncio
import enum
from typing import Awaitable, Callable, Optional, Union

from aqua.interface import parser_plaintext
from aqua.interface.message_types import InputMsg, OutputMsg


class Interface:  # pylint: disable=too-many-instance-attributes
    """
    Interface can format different message types such as JSON or plaintext. By default, plaintext
    is used for readability.

    It maintains a message queue of `OutputMsg`'s or `Awaitable[OutputMsg]`'s that will be outputted
    on each successive call to `output(...)`.
    """

    class FormatType(enum.Enum):
        """Input/output message formats"""

        PLAINTEXT = enum.auto()
        JSON = enum.auto()

    def __init__(
        self, num_workers: int = 8, format_type: FormatType = FormatType.PLAINTEXT
    ):
        if format_type == Interface.FormatType.PLAINTEXT:
            self.parse_input: Callable[
                [str], InputMsg
            ] = parser_plaintext.PlainTextParser.parse_input
            self.serialize_output: Callable[
                [OutputMsg], str
            ] = parser_plaintext.PlainTextParser.serialize_output
        else:
            raise NotImplementedError(f"Unsupported format type: {format_type}")

        self.event_loop: Optional[asyncio.AbstractEventLoop] = None

        self.job_queue: Optional[asyncio.Queue[Union[OutputMsg, Awaitable]]] = None
        self.msg_queue: Optional[asyncio.Queue[OutputMsg]] = None
        self.num_workers = num_workers
        self.tasks: list[asyncio.Task] = []

        self.lookup: dict[str, Callable[[int, ...], Awaitable[None]]] = {
            "ping": self._ping,
            "sleep": self._sleep,
        }

    async def __aenter__(self) -> "Interface":
        self.event_loop = asyncio.get_running_loop()
        self.job_queue = asyncio.Queue()
        self.msg_queue = asyncio.Queue()
        for _ in range(self.num_workers):
            self.tasks.append(self.event_loop.create_task(self._worker_task()))
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        for task in self.tasks:
            task.cancel("interface shutting down")
        self.tasks = []
        self.msg_queue = None
        self.job_queue = None
        self.event_loop = None

    async def _worker_task(self):
        while True:
            msg = await self.job_queue.get()
            if not isinstance(msg, OutputMsg):
                msg = await msg
            await self.msg_queue.put(msg)

    async def input(self, msg: str):
        """
        Asynchronously sends an input message

        :param msg: the msg to send
        """
        input_msg = self.parse_input(msg)
        return_id = input_msg.return_id
        cmd = self.lookup.get(input_msg.request, None)
        if cmd is None:
            await self.job_queue.put(
                OutputMsg(return_id, -1, f"unrecognized request: {input_msg}")
            )
            return
        await cmd(input_msg.return_id, *input_msg.params)

    async def output(self) -> str:
        """
        Asynchronously queries for a response

        :return: a list of responses to output
        """
        msg = await self.msg_queue.get()
        return self.serialize_output(msg)

    async def _ping(self, return_id: int) -> None:
        await self.job_queue.put(OutputMsg(return_id, 0, "pong"))

    async def _sleep(self, return_id: int, seconds: str) -> None:
        seconds = int(seconds)

        async def sleep():
            await asyncio.sleep(seconds)
            return OutputMsg(return_id, 0, "sleep done")

        await self.job_queue.put(self.event_loop.create_task(sleep()))
