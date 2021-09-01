# pylint: disable=missing-function-docstring
"""Tests IMarketData implementations"""
import asyncio
import logging
import warnings
from typing import Any, Awaitable

import pandas as pd
import pytest

from aqua.market_data import AlpacaMarketData, IBKRMarketData, PolygonMarketData, errors
from aqua.market_data.market_data_interface import IMarketData
from aqua.security import Option, Stock

logger = logging.getLogger(__name__)


@pytest.fixture(name="event_loop")
def create_event_loop():
    loop = asyncio.new_event_loop()
    loop.set_debug(True)
    yield loop
    if not loop.is_closed():
        loop.close()


@pytest.fixture(
    params=[
        AlpacaMarketData,
        IBKRMarketData,
        PolygonMarketData,
    ],
    name="market_data_class",
)
def market_data_class_fixture(request):
    return request.param


def test_market_data_name(market_data_class):
    market_data: IMarketData = market_data_class()
    assert isinstance(market_data.name, str)
    assert len(market_data.name) > 0


@pytest.mark.asyncio
async def test_market_data_connection(market_data_class):
    market_data: IMarketData = market_data_class()
    async with market_data:
        await asyncio.sleep(0.1)


async def perform_request(cor: Awaitable) -> Any:
    """Performs an asynchronous market data request and logs warnings or returns result"""
    try:
        res = await cor
        if res is NotImplemented:
            pass
        return res
    except errors.RateLimitError:
        warnings.warn(UserWarning("Rate limit error"))
    except errors.DataPermissionError:
        warnings.warn(UserWarning("Data permission error"))
    return NotImplemented


@pytest.mark.asyncio
async def test_market_data_hist_aapl_daily_bar(market_data_class):
    market_data: IMarketData = market_data_class()
    async with market_data:
        start_date = pd.Timestamp("2021-08-09")
        end_date = pd.Timestamp("2021-08-20")
        res = await perform_request(
            market_data.get_hist_bars(
                Stock("AAPL"), pd.Timedelta("1 day"), start_date, end_date
            )
        )
        if res is NotImplemented:
            return
        assert "Open" in res.columns
        assert "High" in res.columns
        assert "Low" in res.columns
        assert "Close" in res.columns
        assert "Volume" in res.columns
        assert "NumTrades" in res.columns
        assert "VWAP" in res.columns
        assert len(res) == 10  # 10 trading days between start and end date
        assert res.index.min().floor("D") == start_date.tz_localize("America/New_York")
        assert res.index.max().floor("D") == end_date.tz_localize("America/New_York")


@pytest.mark.asyncio
async def test_market_data_hist_aapl_minute_bar(market_data_class):
    market_data: IMarketData = market_data_class()
    async with market_data:
        start_date = pd.Timestamp("2021-08-20")
        end_date = pd.Timestamp("2021-08-20")
        res = await perform_request(
            market_data.get_hist_bars(
                Stock("AAPL"), pd.Timedelta("1 min"), start_date, end_date
            )
        )
        if res is NotImplemented:
            return
        assert "Open" in res.columns
        assert "High" in res.columns
        assert "Low" in res.columns
        assert "Close" in res.columns
        assert "Volume" in res.columns
        assert "NumTrades" in res.columns
        assert "VWAP" in res.columns
        assert len(res) >= 760  # least number of bars to expect
        assert res.index.min().floor("D") == start_date.tz_localize("America/New_York")
        assert res.index.max().floor("D") == end_date.tz_localize("America/New_York")


@pytest.mark.asyncio
async def test_market_data_today_bar(market_data_class):
    market_data: IMarketData = market_data_class()
    async with market_data:
        start_date = pd.Timestamp.now().floor("D")
        end_date = start_date
        res = await perform_request(
            market_data.get_hist_bars(
                Stock("AAPL"), pd.Timedelta("1 hr"), start_date, end_date
            )
        )
        if res is NotImplemented:
            return
        assert "Open" in res.columns
        assert "High" in res.columns
        assert "Low" in res.columns
        assert "Close" in res.columns
        assert "Volume" in res.columns
        assert "NumTrades" in res.columns
        assert "VWAP" in res.columns
        assert len(res) >= 7  # least number of bars to expect
        assert res.index.min().floor("D") == start_date.tz_localize("America/New_York")
        assert res.index.max().floor("D") == end_date.tz_localize("America/New_York")


@pytest.mark.asyncio
async def test_market_data_option(market_data_class):
    market_data: IMarketData = market_data_class()
    async with market_data:
        start_date = pd.Timestamp("2021-08-23")
        end_date = pd.Timestamp("2021-08-27")
        res = await perform_request(
            market_data.get_hist_bars(
                Option(
                    Stock("AAPL"),
                    pd.Timestamp("2021-09-03"),
                    130,
                    Option.Parity.CALL,
                    Option.Type.AMERICAN,
                ),
                pd.Timedelta("1 hr"),
                start_date,
                end_date,
            )
        )
        if res is NotImplemented:
            return
        assert "Open" in res.columns
        assert "High" in res.columns
        assert "Low" in res.columns
        assert "Close" in res.columns
        assert "Volume" in res.columns
        assert "NumTrades" in res.columns
        assert "VWAP" in res.columns
        assert len(res) >= 20  # least number of bars to expect
        assert res.index.min().floor("D") == start_date.tz_localize("America/New_York")
        assert res.index.max().floor("D") == end_date.tz_localize("America/New_York")


@pytest.mark.asyncio
@pytest.mark.live
async def test_market_data_trade_stream(market_data_class):
    market_data: IMarketData = market_data_class()
    streaming_market_data = market_data.get_streaming_market_data()
    if streaming_market_data is NotImplemented:
        return
    async with streaming_market_data:
        await streaming_market_data.subscribe_trades(Stock("SPY"))
        price, size, trade_time = await streaming_market_data.get_trade(Stock("SPY"))
        assert price > 0
        assert size > 0
        assert trade_time < pd.Timestamp.now(tz="America/New_York")
        await streaming_market_data.unsubscribe_trades(Stock("SPY"))


@pytest.mark.asyncio
@pytest.mark.live
async def test_market_data_quote_stream(market_data_class):
    market_data: IMarketData = market_data_class()
    streaming_market_data = market_data.get_streaming_market_data()
    if streaming_market_data is NotImplemented:
        return
    async with streaming_market_data:
        await streaming_market_data.subscribe_quotes(Stock("SPY"))
        (
            (bid_price, bid_size),
            (ask_price, ask_size),
            quote_time,
        ) = await streaming_market_data.get_quote(Stock("SPY"))
        assert bid_price > 0
        assert bid_size > 0
        assert ask_price > 0
        assert ask_size > 0
        assert quote_time < pd.Timestamp.now(tz="America/New_York")
        await streaming_market_data.unsubscribe_quotes(Stock("SPY"))
