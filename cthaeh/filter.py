from typing import NamedTuple, Optional, Tuple, Union

from eth_typing import Address, BlockNumber, Hash32
from eth_utils import to_tuple
from sqlalchemy import orm, or_, Constraint

from cthaeh.models import Block, Header, Log, Receipt, Transaction


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


@to_tuple
def _construct_filters(params: FilterParams) -> Tuple[Constraint, ...]:
    if isinstance(params.address, tuple):
        # TODO: or
        yield or_(*tuple(
            Log.address == address
            for address in params.address
        ))
    elif isinstance(params.address, bytes):
        yield (Log.address == params.address)
    elif params.address is None:
        pass
    else:
        raise TypeError(f"Invalid address parameter: {params.address!r}")

    if isinstance(params.from_block, int):
        yield (Header.block_number >= params.from_block)
    elif params.from_block is None:
        pass
    else:
        raise TypeError(f"Invalid from_block parameter: {params.from_block!r}")

    if isinstance(params.to_block, int):
        yield (Header.block_number <= params.to_block)
    elif params.to_block is None:
        pass
    else:
        raise TypeError(f"Invalid to_block parameter: {params.to_block!r}")


def filter(session: orm.Session, params: FilterParams) -> Tuple[Log, ...]:
    orm_filters = _construct_filters(params)

    return session.query(Log).join(
        Receipt,
        Log.receipt_hash == Receipt.transaction_hash,
    ).join(
        Transaction,
        Receipt.transaction_hash == Transaction.hash,
    ).join(
        Block,
        Transaction.block_header_hash == Block.header_hash,
    ).join(
        Header,
        Block.header_hash == Header.hash,
    ).filter(*orm_filters).all()
