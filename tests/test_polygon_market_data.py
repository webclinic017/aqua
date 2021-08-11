# pylint: disable=missing-module-docstring, missing-function-docstring
import pytest

import pandas as pd

from aqua.market_data.polygon import PolygonMarketData
from aqua.security import Stock


@pytest.mark.asyncio
@pytest.mark.market_data
async def test_get_stocks_by_symbol():
    async with PolygonMarketData() as pmd:
        res = await pmd.get_stocks_by_symbol("SPY")
        assert len(res) == 1
        assert res.pop().symbol == "SPY"


@pytest.mark.asyncio
@pytest.mark.market_data
async def test_get_bar_history_for_spy():
    async with PolygonMarketData() as pmd:
        spy = Stock("SPY")
        res = await pmd.get_stock_bar_history(
            spy,
            pd.Timestamp("2020-08-15"),
            pd.Timestamp("2021-08-09"),
            pd.Timedelta(1, unit="hr"),
        )
        assert len(res) >= 0
        assert "Open" in res.columns
        assert "High" in res.columns
        assert "Low" in res.columns
        assert "Close" in res.columns
        print()
        print(res.head())
        print(res.index.min(), res.index.max())
        print(len(res))
