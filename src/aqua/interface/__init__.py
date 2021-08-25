"""
The `interface` module provides an asynchronous text based class `Interface` for interacting with
aqua.

`Interface` asynchronously consumes input messages and exposes an async function for fetching
output. It supports different message formats with plaintext being the default. However, all
messages formats convey the same information.

Input message:
    return_id (int):    a unique (numerical and positive) id that can be used to match this command
                        with a response. A `return_id` of 0 means that no output is expected.
    request (str):      request to make to aqua (should contain no whitespaces)
    params (list[str]): list of parameters to pass to the given command (each parameter should
                        contain no whitespaces)

Output message:
    return_id (int):    the same unique id passed from a request (or -1 if announcement)
    status (int):       the status of the response
    content (Any):     contents of the response
"""

from aqua.interface.interface import Interface
