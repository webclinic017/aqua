"""JSON interface parser"""
from aqua.interface.messages import InputMsg, OutputMsg
from aqua.interface.parser import IParser


class JSONParser(IParser):
    """
    Input:
    {
        "return_id": <return_id>,
        "request": <request>,
        ["params": <params>]
    }
    Output:
    {
        "return_id": <return_id>,
        "status": <status>,
        "content": <content>
    }
    """

    @staticmethod
    def parse_input(msg: str) -> InputMsg:
        raise NotImplementedError

    @staticmethod
    def serialize_output(output: OutputMsg) -> str:
        raise NotImplementedError
