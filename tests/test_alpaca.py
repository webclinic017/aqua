# pylint: disable=missing-module-docstring, missing-function-docstring
import pandas as pd
import pytest

from aqua.market_data.alpaca import AlpacaMarketData
from aqua.security import Stock


@pytest.mark.asyncio
@pytest.mark.market_data
async def test_get_stocks_by_symbol():
    async with AlpacaMarketData() as market_data:
        res = await market_data.get_stocks_by_symbol("SPY")
        assert len(res) == 1
        assert res.pop().symbol == "SPY"


@pytest.mark.asyncio
@pytest.mark.market_data
async def test_get_bar_history_for_spy():
    async with AlpacaMarketData() as market_data:
        spy = Stock("SPY")
        res = await market_data.get_stock_bar_history(
            spy,
            pd.Timestamp("2020-08-15"),
            pd.Timestamp("2021-08-09"),
            pd.Timedelta(1, unit="hr"),
        )
        print()
        print(res.head())
        print(res.index.min(), res.index.max())
        print(len(res))
        assert not res.empty
        assert res.index.min() == pd.Timestamp(
            "2020-08-17 04:00", tz="America/New_York"
        )
        assert res.index.max() <= pd.Timestamp(
            "2021-08-09 19:00", tz="America/New_York"
        )
        assert "Open" in res.columns
        assert "High" in res.columns
        assert "Low" in res.columns
        assert "Close" in res.columns