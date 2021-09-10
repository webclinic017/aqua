import pandas as pd
import pytest

from aqua.market_data import MarketData
from aqua.market_data.market_data_interface import StreamType
from aqua.security import Option, Stock


@pytest.mark.asyncio
@pytest.mark.live
async def test_stock_quotes():
    stk = Stock("SPY")
    async with MarketData() as market_data:
        await market_data.subscribe(StreamType.QUOTES, stk)
        quote = await market_data.get(StreamType.QUOTES, stk)
        assert quote.ask_size > 0 or quote.bid_size > 0
        print(f"{stk}: ", quote)


@pytest.mark.asyncio
@pytest.mark.live
async def test_option_quotes():
    option = Option(Stock("SPY"), pd.Timestamp("2021-10-01"), 450, Option.Parity.CALL)
    async with MarketData() as market_data:
        await market_data.subscribe(StreamType.QUOTES, option)
        quote = await market_data.get(StreamType.QUOTES, option)
        assert quote.ask_size > 0 or quote.bid_size > 0
        print(f"{option}: ", quote)
