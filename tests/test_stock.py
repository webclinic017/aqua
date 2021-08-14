# pylint: disable=missing-module-docstring, missing-function-docstring
from aqua import security


def test_stock_equality():
    stk1 = security.Stock("AAPL")
    stk2 = security.Stock("aapl")
    assert stk1 == stk2

def test_stock_string_equality():
    stk1 = security.Stock("AAPL")
    assert stk1 == "aapl"
