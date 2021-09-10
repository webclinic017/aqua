from unittest import mock

import pytest

from aqua.internal.ref_counter import AsyncReferenceCounter


@pytest.mark.asyncio
async def test_async_ref_counter():
    init = mock.AsyncMock()
    deinit = mock.AsyncMock()
    ref = AsyncReferenceCounter(init, deinit)
    await ref.new("object")
    init.assert_called_once()
    await ref.new("object")
    init.assert_called_once()
    assert "object" in ref
    await ref.delete("object")
    deinit.assert_not_called()
    await ref.delete("object")
    deinit.assert_called_once()
    assert len(ref.references()) == 0
    assert "object" not in ref
