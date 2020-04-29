from typing import OrderedDict, TypeVar

TKey = TypeVar('TKey')
TValue = TypeVar('TValue')


class LRU(OrderedDict[TKey, TValue]):
    """
    https://docs.python.org/3/library/collections.html#ordereddict-examples-and-recipes
    """
    def __init__(self, max_size: int = 128):
        self.max_size = max_size
        super().__init__()

    def __getitem__(self, key: TKey):
        value = super().__getitem__(key)
        self.move_to_end(key)
        return value

    def __setitem__(self, key: TKey, value: TValue):
        super().__setitem__(key, value)
        if len(self) > self.max_size:
            oldest = next(iter(self))
            del self[oldest]
