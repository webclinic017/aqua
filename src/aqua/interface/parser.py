"""
Parsers for interface
"""
from abc import ABC, abstractmethod

from aqua.interface.message_types import InputMsg, OutputMsg


class IParser(ABC):
    """Interface parser class for input and output messages"""

    @staticmethod
    @abstractmethod
    def parse_input(msg: str) -> InputMsg:
        """
        Parses a raw string into an Interface.InputMsg

        :param msg: the raw string to parse
        :return: an Interface.InputMsg
        :raise ValueError: if the input msg if malformed
        """

    @staticmethod
    @abstractmethod
    def serialize_output(output: OutputMsg) -> str:
        """
        Serializes an Interface.OutputMsg into a string

        :param output: the Interface.OutputMsg to serialize
        :return: a string
        """
