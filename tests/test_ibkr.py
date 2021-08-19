# pylint: disable=missing-module-docstring, missing-function-docstring
import pandas as pd
import pytest

from aqua.market_data import IBKRMarketData
from aqua.security import Option, Stock


@pytest.mark.asyncio
@pytest.mark.market_data
async def test_get_option_prices():
    async with IBKRMarketData() as market_data:
        res = await market_data.get_option_bar_history(
            Option(
                Stock("SPY"),
                pd.Timestamp("2021-08-20"),
                440,
                Option.Parity.CALL,
                Option.Type.AMERICAN,
            ),
            pd.Timestamp("2021-08-10"),
            pd.Timestamp("2021-08-13"),
            pd.Timedelta(1, unit="hr"),
        )
        assert not res.empty
        print()
        print(res)
        print(res.index.min(), res.index.max())
