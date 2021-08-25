"""Plaintext interface parser"""
from aqua.interface.messages import InputMsg, OutputMsg
from aqua.interface.parser import IParser


class PlainTextParser(IParser):
    """
    Input: <return_id> <request> [<params>]
    Output: <return_id> <status>\n<content>
    """

    @staticmethod
    def parse_input(msg: str) -> InputMsg:
        split_msg = msg.split()
        if len(split_msg) < 2:
            raise ValueError(f"Invalid msg: too few tokens {msg}")
        try:
            return_id = int(split_msg[0])
        except ValueError as int_parse_exception:
            raise ValueError(
                f"Invalid msg: can't parse return id {split_msg[0]}"
            ) from int_parse_exception
        request = split_msg[1]
        params = split_msg[2:]
        return InputMsg(return_id, request, params)

    @staticmethod
    def serialize_output(output: OutputMsg) -> str:
        return f"{output.return_id} {output.status}\n{output.content}"
