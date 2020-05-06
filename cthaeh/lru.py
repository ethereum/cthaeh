import collections
from typing import MutableMapping, TypeVar

TKey = TypeVar("TKey")
TValue = TypeVar("TValue")


class LRU(collections.OrderedDict, MutableMapping[TKey, TValue]):  # type: ignore
    """
    https://docs.python.org/3/library/collections.html#ordereddict-examples-and-recipes
    """

    def __init__(self, max_size: int = 128) -> None:
        self.max_size = max_size
        super().__init__()

    def __getitem__(self, key: TKey) -> TValue:
        value = super().__getitem__(key)
        self.move_to_end(key)
        return value  # type: ignore

    def __setitem__(self, key: TKey, value: TValue) -> None:
        super().__setitem__(key, value)
        if len(self) > self.max_size:
            oldest = next(iter(self))
            del self[oldest]
