"""
Defines all the commands supported by interface
"""
import asyncio
from typing import Awaitable, Callable, Union

from aqua.interface.message_types import OutputMsg

MsgQueue = asyncio.Queue[Union[OutputMsg, Awaitable]]


async def ping(msg_queue: MsgQueue, return_id: int) -> None:
    await msg_queue.put(OutputMsg(return_id, 0, "pong"))


lookup: dict[str, Callable[[MsgQueue, int, ...], Awaitable[None]]] = {"ping": ping}
