"""
Defines a stock contract
"""


class Stock:
    """
    A stock represents a share of a company or index.
    We assume that it can be uniquely defined by a symbol (ticker) and ISIN.
    """

    def __init__(self, symbol: str, isin: str) -> None:
        self.symbol = symbol
        self.isin = isin

    def __eq__(self, o: object) -> bool:
        if isinstance(o, Stock):
            return self.symbol == o.symbol and self.isin == o.isin
        return NotImplemented

    def __hash__(self) -> int:
        return hash((self.symbol, self.isin))
