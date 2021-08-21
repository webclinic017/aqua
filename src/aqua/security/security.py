"""
A security is anything that has monetary value and can be traded
"""
from abc import ABC, abstractmethod


class Security(ABC):
    """
    A base class for any security
    """

    @abstractmethod
    def __hash__(self):
        pass

    @abstractmethod
    def __eq__(self, other):
        pass

    @abstractmethod
    def __repr__(self):
        pass
