import contextlib
import cProfile
from typing import (
    Iterator,
)


@contextlib.contextmanager
def profiler(filename: str) -> Iterator[None]:
    pr = cProfile.Profile()
    pr.enable()
    try:
        yield
    finally:
        pr.disable()
        pr.dump_stats(filename)
