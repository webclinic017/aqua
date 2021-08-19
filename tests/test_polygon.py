# pylint: disable=missing-module-docstring, missing-function-docstring
import pandas as pd
import pytest

from aqua.market_data import PolygonMarketData
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
        assert not res.empty
        assert res.index.min() <= pd.Timestamp(
            "2020-08-17 04:00", tz="America/New_York"
        )
        assert res.index.max() >= pd.Timestamp(
            "2021-08-09 19:00", tz="America/New_York"
        )
        assert "Open" in res.columns
        assert "High" in res.columns
        assert "Low" in res.columns
        assert "Close" in res.columns


@pytest.mark.asyncio
@pytest.mark.market_data
async def test_get_dividends_for_aapl():
    async with PolygonMarketData() as pmd:
        res = await pmd.get_stock_dividends(Stock("AAPL"))
        assert not res.empty


@pytest.mark.asyncio
@pytest.mark.market_data
async def test_get_splits_for_aapl():
    async with PolygonMarketData() as pmd:
        res = await pmd.get_stock_splits(Stock("AAPL"))
        assert not res.empty
