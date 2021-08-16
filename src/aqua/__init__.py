"""
`aqua` is A Quantitative Trading framework for fetching market data, processing market data,
and managing orders and strategies.

It consists of the following submodules.

The `security` module contains definitions for the securities used by `aqua` such as `Stock`,
`Option`, etc.

The `market_data` submodule contains functionality for fetching market data such as historical
trades, quotes, bars, company financials, etc. as well as live data.
"""

import aqua.market_data
import aqua.security
