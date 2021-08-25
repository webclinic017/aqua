"""
The Interface implementation
"""
import asyncio
import enum
from typing import Callable, Optional

from aqua.interface import parser_plaintext
from aqua.interface.messages import InputMsg, OutputMsg


class Interface:
    """
    Interface can format different message types such as JSON or plaintext. By default, plaintext
    is used for readability.
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

        self.announcement_queue: Optional[asyncio.Queue[OutputMsg]] = None

    async def __aenter__(self) -> "Interface":
        self.announcement_queue = asyncio.Queue()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.announcement_queue = None

    async def input(self, msg: str):
        """
        Asynchronously sends an input message.

        :param msg: the msg to send
        """
        input_msg = self.parse_input(msg)
        return_id = input_msg.return_id
        req = input_msg.request.lower()
        if req == "ping":
            await self.announcement_queue.put(OutputMsg(return_id, 0, "pong"))
        else:
            await self.announcement_queue.put(
                OutputMsg(return_id, -1, f"unrecognized request: {req}")
            )

    async def output(self) -> list[str]:
        """
        Asynchronously queries for a list of responses.

        :return: a list of responses to output
        """
        announcement_task = asyncio.create_task(self.announcement_queue.get())
        output_tasks, _ = await asyncio.wait(
            [announcement_task], return_when=asyncio.FIRST_COMPLETED
        )
        return [
            self.serialize_output(output_task.result()) for output_task in output_tasks
        ]
