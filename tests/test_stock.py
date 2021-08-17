# pylint: disable=missing-module-docstring, missing-function-docstring
from aqua.security import Stock


def test_stock_equality():
    stk1 = Stock("AAPL")
    stk2 = Stock("aapl")
    assert stk1 == stk2


def test_stock_string_equality():
    stk1 = Stock("AAPL")
    assert stk1 == "aapl"
