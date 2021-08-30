"""
The broker_interface defines the IBroker interface, which communicates with a broker for retrieving
account data or placing orders
"""
from abc import ABC, abstractmethod
from typing import Tuple

import pandas as pd

from aqua.portfolio import Portfolio


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
    async def get_portfolio_updates(self) -> Tuple[Portfolio, pd.Timestamp]:
        """
        Returns an asyncio Queue that can be queried for Portfolio updates.

        The elements of the queue are tuples (Portfolio, Timestamp) which represents the portfolio
        updated at the given Timestamp.

        :return: an asyncio Queue
        """
        raise NotImplementedError
