"""Defines input/output messages for interface"""
from typing import Any


class InputMsg:  # pylint: disable=too-few-public-methods
    """Input message for interface"""

    def __init__(self, return_id: int, request: str, params: list[str] = None):
        if params is None:
            params = []
        if return_id < 0:
            raise ValueError(f"Return id's must be nonnegative. Got {return_id}")
        self.return_id = return_id
        if any(x.isspace() for x in request):
            raise ValueError(
                f"Request must not contain any whitespaces. Got {request}"
            )
        self.request = request
        for param in params:
            if any(x.isspace() for x in param):
                raise ValueError(
                    f"Parameters must not contain any whitespaces. Got {params}"
                )
        self.params = params


class OutputMsg:  # pylint: disable=too-few-public-methods
    """Output message for interface"""

    def __init__(self, return_id: int, status: int, content: Any):
        self.return_id = return_id
        self.status = status
        self.content = content
