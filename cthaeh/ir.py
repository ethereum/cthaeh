from dataclasses import dataclass
import functools
from typing import Any, NamedTuple, Optional, Tuple

from eth_typing import Address, Hash32
from eth_utils import humanize_hash, to_checksum_address


@dataclass
@functools.total_ordering
class Header:
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

    def __repr__(self) -> str:
        return (
            f"Header("
            f"is_canonical={self.is_canonical!r}, "
            f"hash={self.hash!r}, "
            f"parent_hash={self.parent_hash!r}, "
            f"uncles_hash={self.uncles_hash!r}, "
            f"coinbase={self.coinbase!r}, "
            f"state_root={self.state_root!r}, "
            f"transaction_root={self.transaction_root!r}, "
            f"receipt_root={self.receipt_root!r}, "
            f"bloom={self.bloom!r}, "
            f"difficulty={self.difficulty!r}, "
            f"block_number={self.block_number!r}, "
            f"gas_limit={self.gas_limit!r}, "
            f"gas_used={self.gas_used!r}, "
            f"timestamp={self.timestamp!r}, "
            f"extra_data={self.extra_data!r}, "
            f"nonce={self.nonce!r}, "
            "("
        )

    def __str__(self) -> str:
        return f"Header[#{self.block_number} {humanize_hash(self.hash)}]"

    @property
    def is_genesis(self) -> bool:
        return self.block_number == 0

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, type(self)):
            return False
        return self.block_number == other.block_number

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, type(self)):
            raise TypeError(f"Cannot compare Header to {other!r}")
        return self.block_number < other.block_number


@dataclass
class Transaction:
    hash: Hash32
    nonce: int
    gas_price: int
    gas: int
    to: Optional[Address]
    value: bytes
    data: bytes
    v: bytes
    r: bytes
    s: bytes
    sender: Address


@dataclass
class Log:
    address: Address
    topics: Tuple[Hash32, ...]
    data: bytes

    def __repr__(self) -> str:
        return (
            f"Log(address={self.address!r}, topics={self.topics!r}, data={self.data!r})"
        )  # noqa: E501

    def __str__(self) -> str:
        return (
            f"Log("  # type: ignore
            f"address={to_checksum_address(self.address)}, "
            f"topics={tuple(humanize_hash(topic) for topic in self.topics)}, "
            f"data={humanize_hash(self.data)}"
            ")"
        )


@dataclass
class Receipt:
    state_root: Hash32
    gas_used: int
    bloom: bytes
    logs: Tuple[Log, ...]

    def __repr__(self) -> str:
        return f"Receipt(state_root={self.state_root!r}, gas_used={self.gas_used!r}, bloom={self.bloom!r}, logs={self.logs!r})"  # noqa: E501

    def __str__(self) -> str:
        return (
            f"Receipt("  # type: ignore
            f"state_root={humanize_hash(self.state_root)}, "
            f"gas_used={self.gas_used}, "
            f"bloom={humanize_hash(self.bloom)}, "
            f"logs={tuple(str(log) for log in self.logs)}"
            ")"
        )


@functools.total_ordering
@dataclass
class Block:
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
        return f"Block[#{self.header.block_number} {humanize_hash(self.header.hash)}]"


class HeadBlockPacket(NamedTuple):
    blocks: Tuple[Block, ...]
    orphans: Tuple[Header, ...] = ()
