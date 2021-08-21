"""
Defines an option contract
"""
from enum import Enum, auto
from typing import Union

import pandas as pd

from aqua.security.security import Security
from aqua.security.stock import Stock


class Option(Security):
    """
    A generic option for any underlying.
    """

    class Parity(Enum):
        """
        Call or put option.
        Call: right to buy at strike price
        Put: right to sell at strike price
        """

        CALL = auto()
        PUT = auto()

        def __lt__(self, other):
            if isinstance(other, Option.Parity):
                return self == Option.Parity.CALL and other == Option.Parity.PUT
            return NotImplemented

    class Type(Enum):
        """
        American or European option
        American: right to exercise whenever
        European: right to exercise only at expiration
        """

        AMERICAN = auto()
        EUROPEAN = auto()

        def __lt__(self, other):
            if isinstance(other, Option.Type):
                return self == Option.Type.AMERICAN and other == Option.Type.EUROPEAN
            return NotImplemented

    def __init__(
        self,
        underlying: Union[Stock],
        expiration: pd.Timestamp,
        strike: float,
        parity: Parity,
        option_type: Type,
    ):
        self.underlying = underlying
        self.expiration = expiration
        self.strike = strike
        self.parity = parity
        self.option_type = option_type

    def __hash__(self):
        return hash(self._as_tuple())

    def __eq__(self, other):
        if isinstance(other, Option):
            return self._as_tuple() == other._as_tuple()
        return NotImplemented

    def __lt__(self, other):
        if isinstance(other, Option):
            return self._as_tuple() < other._as_tuple()
        return NotImplemented

    def _as_tuple(self) -> tuple[Union[Stock], pd.Timestamp, float, Parity, Type]:
        return (
            self.underlying,
            self.expiration,
            self.strike,
            self.parity,
            self.option_type,
        )
