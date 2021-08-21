# pylint: disable=missing-module-docstring, missing-function-docstring
from aqua.portfolio.portfolio import Portfolio
from aqua.portfolio.strategy import Strategy
from aqua.security import Stock


def test_agg_positions():
    portfolio = Portfolio(
        [
            Strategy(positions={Stock("AAPL"): 2}),
            Strategy(positions={Stock("AAPL"): 6}),
        ]
    )
    assert portfolio.positions == {Stock("AAPL"): 8}
