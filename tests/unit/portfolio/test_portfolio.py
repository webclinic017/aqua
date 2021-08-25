# pylint: disable=missing-module-docstring, missing-function-docstring
import pytest

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


def test_portfolio_getitem_setitem():
    portfolio = Portfolio(
        [
            Strategy(name="strategy 1", positions={Stock("AAPL"): 2}),
        ]
    )
    assert portfolio["strategy 1"].positions == {Stock("AAPL"): 2}
    portfolio["strategy 2"] = Strategy(name="strategy 1", positions={Stock("MSFT"): -1})
    assert portfolio["strategy 2"].positions == {Stock("MSFT"): -1}

    with pytest.raises(TypeError):
        _ = portfolio[3]
    with pytest.raises(TypeError):
        portfolio[3] = Strategy()
    with pytest.raises(TypeError):
        portfolio["default"] = 3


def test_empty_portfolio():
    portfolio = Portfolio()
    assert portfolio.cash == 0
    assert len(portfolio.strategies) == 0
