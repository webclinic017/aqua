"""
The broker_interface defines the IBroker interface, which communicates with a broker for retrieving
account data or placing orders
"""
from abc import ABC, abstractmethod
from typing import Tuple

from aqua.security.security import Security


class IBroker(ABC):
    """
    Specifies an interface for interacting with a broker
    """

    @abstractmethod
    async def __aenter__(self):
        raise NotImplementedError

    @abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        raise NotImplementedError

    @abstractmethod
    async def subscribe(self):
        """
        Subscribes to broker account updates
        :return:
        """
        raise NotImplementedError

    @abstractmethod
    async def get_positions(self) -> Tuple[dict[Security, float], float]:
        """
        Asynchronously fetches a tuple (positions, cash balance) where positions is a mapping
        from securities to their positions (guaranteed to be nonzero).
        :return: a tuple (positions, cash balance)
        """
        raise NotImplementedError

    @abstractmethod
    async def unsubscribe(self):
        """
        Unsubscribes from broker account updates
        :return:
        """
        raise NotImplementedError
