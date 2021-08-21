"""
A strategy is a defined by the set of positions that result from a sequence of trades
made under the same guiding principles (i.e. the trading strategy).
"""

from collections import defaultdict
from typing import Optional

from aqua.security.security import Security


class Strategy:
    """
    Defines a strategy
    """

    def __init__(
        self,
        name: str = "",
        positions: Optional[dict[Security, float]] = None,
    ):
        self.name = name
        self.positions = positions
        if self.positions is None:
            self.positions = dict()
        self._prune()

    def __add__(self, other):
        if isinstance(other, Strategy):
            tot_pos = defaultdict(float, self.positions)
            for con, qty in other.positions:
                tot_pos[con] += qty
            name = f"{self.name} + {other.name}"
            return Strategy(name, dict(tot_pos))
        return NotImplemented

    def __getitem__(self, item):
        if not isinstance(item, Security):
            raise TypeError(f"Expected key to be Security type. Got type {type(item)}")
        return self.positions.get(item, 0)

    def __setitem__(self, key, value):
        if not isinstance(key, Security):
            raise TypeError(f"Expected key to be Security type. Got type {type(key)}")
        value = float(value)
        if value != 0:
            self.positions[key] = value
        else:
            del self.positions[key]

    def __delitem__(self, key):
        del self.positions[key]

    def _prune(self):
        self.positions = {
            key: value for key, value in self.positions.items() if value != 0
        }
