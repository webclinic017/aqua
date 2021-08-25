# pylint: disable=missing-module-docstring, missing-function-docstring
import pandas as pd
import pytest

from aqua.portfolio.strategy import Strategy
from aqua.security import Option, Stock


def test_default_init():
    name = "some name"
    strat = Strategy(name)
    assert strat.name == name
    assert len(strat.positions) == 0


def test_add_position():
    strat = Strategy()
    strat[Stock("AAPL")] = 6
    option = Option(
        Stock("AAPL"),
        pd.Timestamp("2021-01-01"),
        100,
        Option.Parity.CALL,
        Option.Type.AMERICAN,
    )
    strat[option] = -1
    assert strat[Stock("AAPL")] == 6
    assert strat[option] == -1


def test_del_position():
    strat = Strategy(positions={Stock("AAPL"): 6})
    strat[Stock("AAPL")] = 0
    assert Stock("AAPL") not in strat.positions
    strat = Strategy(positions={Stock("AAPL"): 6})
    del strat[Stock("AAPL")]
    assert Stock("AAPL") not in strat.positions


def test_add_strategy():
    option = Option(
        Stock("AAPL"),
        pd.Timestamp("2021-01-01"),
        100,
        Option.Parity.CALL,
        Option.Type.AMERICAN,
    )
    strat1 = Strategy(positions={Stock("AAPL"): 6, option: -1})
    strat2 = Strategy(positions={Stock("AAPL"): 2, option: 5})
    strat = strat1 + strat2
    assert strat[Stock("AAPL")] == 8
    assert strat[option] == 4


def test_type_error():
    strat = Strategy()
    with pytest.raises(TypeError):
        strat["foo"] = 5
    with pytest.raises(TypeError):
        del strat["foo"]
    with pytest.raises(TypeError):
        _ = strat["foo"]
    with pytest.raises(TypeError):
        Strategy(positions={"foo": 5})


def test_empty_position():
    strat = Strategy(positions={Stock("AAPL"): 0})
    assert strat[Stock("AAPL")] == 0
