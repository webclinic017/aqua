"""
Provides common errors/exceptions for market data sources
"""


class DataSourceError(Exception):
    """
    Data source error occurs when the data source (polygon, ibkr, etc.) service returns a response
    that can't be handled
    """


class CredentialError(Exception):
    """
    Credential error occurs when the data source does not have permission to access the desired data
    """


class ConfigError(Exception):
    """
    Config error occurs when the market data classes are not configured correctly.
    This is usually the result of improperly set environment variables
    """


class RateLimitError(Exception):
    """
    Rate limit errors happen if too many requests are made to a service.
    """


class DataPermissionError(Exception):
    """
    Data permission error happens if the user lacks permissions to access the requested data
    """
