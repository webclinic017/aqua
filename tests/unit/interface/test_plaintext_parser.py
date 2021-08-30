# pylint: disable=missing-module-docstring, missing-function-docstring
import pytest

from aqua.interface.parser_plaintext import PlainTextParser


def test_parse_normal_msg():
    inp = PlainTextParser.parse_input("0 request foo")
    assert inp.request == "request"
    assert inp.return_id == 0
    assert inp.params == ["foo"]


def test_parse_invalid_return_id():
    with pytest.raises(ValueError):
        PlainTextParser.parse_input("hi request foo")


def test_parse_too_short():
    with pytest.raises(ValueError):
        PlainTextParser.parse_input("hi")
