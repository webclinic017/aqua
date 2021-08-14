"""
Defines a stock contract
"""


class Stock:
    """
    A stock represents a share of a company or index.
    We assume that it can be uniquely defined by a symbol (ticker) and ISIN.
    """

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol.upper()

    def __eq__(self, o: object) -> bool:
        if isinstance(o, Stock):
            return self.symbol == o.symbol
        if isinstance(o, str):
            return self.symbol == o.upper()
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.symbol)

    def __repr__(self) -> str:
        return f"stock: {self.symbol}"
