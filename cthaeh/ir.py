from typing import NamedTuple, Optional, Tuple

from eth_typing import Address, Hash32


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


class Receipt(NamedTuple):
    state_root: Hash32
    gas_used: int
    bloom: bytes
    logs: Tuple[Log, ...]


class Block(NamedTuple):
    header: Header
    transactions: Tuple[Transaction, ...]
    uncles: Tuple[Header, ...]
    receipts: Tuple[Receipt, ...]
