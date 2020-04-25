from typing import NamedTuple, Optional, Tuple, Union

from eth_typing import Address, BlockNumber, Hash32
from sqlalchemy import orm

from cthaeh.models import Log, Receipt


BlockIdentifier = BlockNumber
# TODO: update to python3.8
# BlockIdentifier = Union[
#     Literal['latest'],
#     Literal['pending'],
#     Literal['earliest'],
#     BlockNumber,
# ]
Topic = Union[None, Hash32, Tuple[Hash32, ...]]


class FilterParams(NamedTuple):
    from_block: Optional[BlockIdentifier] = None
    to_block: Optional[BlockIdentifier] = None
    address: Union[None, Address, Tuple[Address, ...]] = None
    topics: Tuple[Topic, ...] = ()


def filter(session: orm.Session, params: FilterParams) -> Tuple[Log, ...]:
    if isinstance(params.address, tuple):
        address_filters = tuple(
            Log.address == address
            for address in params.address
        )
    else:
        address_filters = ((Log.address == params.address),)

    return session.query(Log).join(
        Receipt,
        Log.receipt_hash == Receipt.transaction_hash
    ).filter(
        *address_filters
    ).all()
