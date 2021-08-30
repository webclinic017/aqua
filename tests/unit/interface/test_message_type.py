# pylint: disable=missing-module-docstring, missing-function-docstring
import pytest

from aqua.interface.message_types import InputMsg, OutputMsg


def test_input_msg_neg_return_id():
    with pytest.raises(ValueError):
        InputMsg(-1, "", [])


def test_input_msg_space_request():
    with pytest.raises(ValueError):
        InputMsg(0, "foo bar")


def test_input_msg_space_params():
    with pytest.raises(ValueError):
        InputMsg(0, "", ["foo bar"])
