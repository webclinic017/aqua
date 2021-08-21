"""
The portfolio module keeps track of the current portfolio and the strategies within it.

A portfolio is defined as a set of strategies with a dollar allocation to each strategy.
Through leverage, some strategies may employ more dollars than what is allocated to it.

A strategy is defined as a set of positions where a position is a tradable contract
(like a share of stock or an option) and a quantity associated with it.

This module contains a portfolio manager, which
1. keeps track of a portfolio and its strategies
2. creates/deletes strategies within the portfolio
3. uses an "order engine" to place and track orders within each strategy
"""
