"""
The Interface implementation
"""
import asyncio
import enum
from typing import Awaitable, Callable, Optional, Union

from aqua.interface import commands, parser_plaintext
from aqua.interface.message_types import InputMsg, OutputMsg


class Interface:
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

    def __init__(self, format_type: FormatType = FormatType.PLAINTEXT):
        if format_type == Interface.FormatType.PLAINTEXT:
            self.parse_input: Callable[
                [str], InputMsg
            ] = parser_plaintext.PlainTextParser.parse_input
            self.serialize_output: Callable[
                [OutputMsg], str
            ] = parser_plaintext.PlainTextParser.serialize_output
        else:
            raise NotImplementedError(f"Unsupported format type: {format_type}")

        self.msg_queue: Optional[asyncio.Queue[Union[OutputMsg, Awaitable]]] = None

    async def __aenter__(self) -> "Interface":
        self.msg_queue = asyncio.Queue()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.msg_queue = None

    async def input(self, msg: str):
        """
        Asynchronously sends an input message.

        :param msg: the msg to send
        """
        input_msg = self.parse_input(msg)
        return_id = input_msg.return_id
        cmd = commands.lookup.get(input_msg.request, None)
        if cmd is None:
            await self.msg_queue.put(
                OutputMsg(return_id, -1, f"unrecognized request: {input_msg}")
            )
            return
        await cmd(self.msg_queue, input_msg.return_id, *input_msg.params)

    async def output(self) -> str:
        """
        Asynchronously queries for a list of responses.

        :return: a list of responses to output
        """
        msg = await self.msg_queue.get()
        if not isinstance(msg, OutputMsg):
            msg = await msg
        return self.serialize_output(msg)
