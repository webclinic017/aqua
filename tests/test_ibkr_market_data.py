# pylint: disable=missing-module-docstring, missing-function-docstring
import pandas as pd
import pytest

from aqua.market_data import IBKRMarketData
from aqua.security import Option, Stock


@pytest.mark.asyncio
@pytest.mark.market_data
@pytest.mark.ibkr
async def test_get_option_prices():
    async with IBKRMarketData() as market_data:
        res = await market_data.get_option_bar_history(
            Option(
                Stock("SPY"),
                pd.Timestamp("2021-12-31"),
                440,
                Option.Parity.CALL,
                Option.Type.AMERICAN,
            ),
            pd.Timestamp("2021-08-15"),
            pd.Timestamp("2021-08-20"),
            pd.Timedelta(4, unit="hr"),
        )
        assert not res.empty
        assert res.index.min() >= pd.Timestamp("2021-08-15", tz="America/New_York")
        assert res.index.max() <= pd.Timestamp("2021-08-20", tz="America/New_York")
