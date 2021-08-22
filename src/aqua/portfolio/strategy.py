"""
A strategy is a defined by the set of positions that result from a sequence of trades
made under the same guiding principles (i.e. the trading strategy).
"""

from collections import defaultdict
from typing import Optional

from aqua.security import Option, Stock
from aqua.security.security import Security


class Strategy:
    """
    Defines a strategy
    """

    def __init__(
        self,
        name: str = "default",
        positions: Optional[dict[Security, float]] = None,
    ):
        self.name = name
        if positions is None:
            positions = dict()
        self.positions: dict[Security, float] = {}
        for sec, qty in positions.items():
            if not isinstance(sec, Security):
                raise TypeError(f"Expected security. Got {type(sec)}")
            qty = float(qty)
            if qty == 0:
                continue
            self.positions[sec] = qty

    def __add__(self, other):
        if isinstance(other, Strategy):
            tot_pos = defaultdict(float, self.positions)
            for con, qty in other.positions.items():
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
            if key in self.positions:
                del self.positions[key]

    def __delitem__(self, key):
        if not isinstance(key, Security):
            raise TypeError(f"Expected key to be Security type. Got type {type(key)}")
        del self.positions[key]

    def __repr__(self):
        def _security_sort_key(sec: Security) -> str:
            if isinstance(sec, Stock):
                return sec.symbol
            if isinstance(sec, Option):
                return f"{sec.underlying.symbol} {sec.expiration.strftime('%Y%m%d')}"
            return ""

        lines = []
        positions = sorted(self.positions.keys(), key=_security_sort_key)
        max_len = max(map(lambda x: len(repr(x)), positions))
        for pos in positions:
            sec_repr = repr(pos)
            qty = self.positions[pos]
            lines.append(f"| {sec_repr.rjust(max_len)} : {qty}")
        max_len = max(map(len, lines), default=0)
        lines = [f"{x.ljust(max_len)} |" for x in lines]
        lines.insert(0, self.name.center(max(len(self.name), max_len), "-"))
        return "\n".join(lines)
