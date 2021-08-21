"""
The `market_data` module contains functionality for fetching market data.
"""

from aqua.market_data.aggregate import MarketData
from aqua.market_data.alpaca import AlpacaMarketData
from aqua.market_data.errors import CredentialError, DataSourceError
from aqua.market_data.ibkr import IBKRMarketData
from aqua.market_data.polygon import PolygonMarketData
