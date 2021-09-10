"""
AsyncReferenceCounter allows us to specify a custom initializer and de-initializer that are
called when a reference count goes from 0 to 1 or from 1 to 0.
"""
from typing import Awaitable, Callable, Generic, TypeVar

Obj = TypeVar("Obj")


class AsyncReferenceCounter(Generic[Obj]):
    """
    AsyncReferenceCounter takes a initializer and de-initializer as arguments. These functions
    are called when a new object needs to be initialized or when an old object is no longer needed.
    """

    def __init__(
        self,
        init: Callable[[Obj], Awaitable[None]],
        deinit: Callable[[Obj], Awaitable[None]],
    ):
        self.init = init
        self.deinit = deinit
        self.ref_counts: dict[Obj, int] = {}

    async def new(self, obj: Obj):
        """
        Increases an object's reference count. If it's a new object, call `init(...)` on it.
        :param obj: obj to increase reference count
        """
        if obj not in self.ref_counts:
            self.ref_counts[obj] = 1
            await self.init(obj)
        else:
            self.ref_counts[obj] += 1

    async def delete(self, obj: Obj):
        """
        Decreases an object's reference count. If the new reference count is 0, call
        `deinit(...)` on it.
        :param obj: obj to decrease reference count
        """
        if self.ref_counts[obj] == 1:
            del self.ref_counts[obj]
            await self.deinit(obj)
        else:
            self.ref_counts[obj] -= 1

    def references(self) -> frozenset[Obj]:
        return frozenset(self.ref_counts.keys())

    def __contains__(self, item):
        return item in self.ref_counts
