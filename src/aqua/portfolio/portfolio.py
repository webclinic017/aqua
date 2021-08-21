"""
A portfolio is a collection of strategies
"""
from collections import defaultdict
from typing import Iterable

from aqua.portfolio.strategy import Strategy
from aqua.security.security import Security


class Portfolio:
    """
    The portfolio class holds mapping of names to strategies.
    """

    def __init__(self, strategies: Iterable[Strategy]):
        self.strategies = {}
        for strat in strategies:
            if strat.name in self.strategies:
                self.strategies[strat.name] += strat
            else:
                self.strategies[strat.name] = strat

    @property
    def positions(self) -> dict[Security, float]:
        """
        Returns the aggregate positions in a portfolio (across all strategies)
        @return: a dictionary mapping each position to its quantity
        """
        pos = defaultdict(float)
        for strat in self.strategies.values():
            for sec, qty in strat.positions.items():
                pos[sec] += qty
        return pos

    def __repr__(self):
        if len(self.strategies) == 0:
            return ""
        strategies = sorted(
            self.strategies.values(), key=lambda x: (x.name, len(x.positions))
        )
        reprs = list(map(repr, strategies))
        max_len = max(map(lambda x: max(map(len, x.split("\n")), default=0), reprs))
        separator = "\n" + "=" * max_len + "\n"
        res = separator.join(reprs)
        return res
