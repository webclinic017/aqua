# pylint: disable=missing-module-docstring, missing-function-docstring
import pandas as pd

from aqua.security import Option, Stock


def test_option_equality():
    opt1 = Option(
        Stock("AAPL"),
        pd.Timestamp("2021-01-01"),
        100,
        Option.Parity.CALL,
        Option.Type.AMERICAN,
    )
    opt2 = Option(
        Stock("aapl"),
        pd.Timestamp("2021-01-01"),
        100,
        Option.Parity.CALL,
        Option.Type.AMERICAN,
    )
    assert opt1 == opt2


def test_option_inequality():
    opt1 = Option(
        Stock("AAPL"),
        pd.Timestamp("2021-01-01"),
        100,
        Option.Parity.CALL,
        Option.Type.AMERICAN,
    )
    opt2 = Option(
        Stock("aapl"),
        pd.Timestamp("2021-01-05"),
        100,
        Option.Parity.CALL,
        Option.Type.AMERICAN,
    )
    assert opt1 != opt2
