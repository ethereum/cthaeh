import functools
from typing import Any, NamedTuple, Optional, Tuple

from eth_typing import Address, Hash32
from eth_utils import humanize_hash, to_checksum_address


class Header(NamedTuple):
    is_canonical: bool

    hash: Hash32
    parent_hash: Hash32
    uncles_hash: Hash32
    coinbase: Address
    state_root: Hash32
    transaction_root: Hash32
    receipt_root: Hash32
    bloom: bytes
    difficulty: bytes
    block_number: int
    gas_limit: int
    gas_used: int
    timestamp: int
    extra_data: bytes
    # mix_hash: Hash32
    nonce: bytes

    @property
    def is_genesis(self) -> bool:
        return self.block_number == 0


class Transaction(NamedTuple):
    hash: Hash32
    nonce: int
    gas_price: int
    gas: int
    to: Optional[Address]
    value: bytes
    data: bytes
    v: int
    r: int
    s: int
    sender: Address


class Log(NamedTuple):
    address: Address
    topics: Tuple[Hash32, ...]
    data: bytes

    def __repr__(self) -> str:
        return f"Log(address={self.address!r}, topics={self.topics!r}, data={self.data!r})"  # noqa: E501

    def __str__(self) -> str:
        return (
            f"Log("
            f"address={to_checksum_address(self.address)}, "
            f"topics={tuple(humanize_hash(topic) for topic in self.topics)}, "
            f"data={humanize_hash(self.data)}"
            ")"
        )


class Receipt(NamedTuple):
    state_root: Hash32
    gas_used: int
    bloom: bytes
    logs: Tuple[Log, ...]

    def __repr__(self) -> str:
        return f"Receipt(state_root={self.state_root!r}, gas_used={self.gas_used!r}, bloom={self.bloom!r}, logs={self.logs!r})"  # noqa: E501

    def __str__(self) -> str:
        return (
            f"Receipt("
            f"state_root={humanize_hash(self.state_root)}, "
            f"gas_used={self.gas_used}, "
            f"bloom={humanize_hash(self.bloom)}, "
            f"logs={tuple(str(log) for log in self.logs)}"
            ")"
        )


@functools.total_ordering
class Block(NamedTuple):
    header: Header
    transactions: Tuple[Transaction, ...]
    uncles: Tuple[Header, ...]
    receipts: Tuple[Receipt, ...]

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, type(self)):
            return False
        return self.header.hash == other.header.hash

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, type(self)):
            raise TypeError(f"Cannot compare Block to {other!r}")
        return self.header.block_number < other.header.block_number

    def __repr__(self) -> str:
        return f"Block(header={self.header!r}, transactions={self.transactions!r}, uncles={self.uncles!r}, receipts={self.receipts!r})"  # noqa: E501

    def __str__(self) -> str:
        return f"Block[{humanize_hash(self.header.hash)}]"
