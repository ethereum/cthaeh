from typing import Optional

from eth_typing import Hash32
from eth_utils import big_endian_to_int, int_to_big_endian, humanize_hash
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    Column,
    LargeBinary,
    BigInteger,
    Integer,
    Boolean,
    ForeignKey,
    UniqueConstraint,
    Index,
)
from sqlalchemy import orm
from sqlalchemy.orm import relationship, backref

from cthaeh.constants import GENESIS_PARENT_HASH
from cthaeh.ir import (
    Header as HeaderIR,
    Transaction as TransactionIR,
    Receipt as ReceiptIR,
    Log as LogIR,
)
from cthaeh.session import Session

Base = declarative_base()


class BlockUncle(Base):
    query = Session.query_property()

    __tablename__ = 'blockuncle'
    __table_args__ = (
        Index("ix_blockuncle_idx_block_header_hash", "idx", "block_header_hash", unique=True),
        Index("ix_block_header_hash_uncle_hash", "block_header_hash", "uncle_hash", unique=True),
    )

    idx = Column(Integer, nullable=False)

    block_header_hash = Column(LargeBinary(32), ForeignKey('block.header_hash'), primary_key=True)
    uncle_hash = Column(LargeBinary(32), ForeignKey('header.hash'), primary_key=True)

    block = relationship("Block")
    uncle = relationship("Header")


class Header(Base):
    query = Session.query_property()

    __tablename__ = 'header'

    hash = Column(LargeBinary(32), primary_key=True)

    block = relationship("Block", uselist=False, back_populates="header")
    uncle_blocks = relationship(
        "Block",
        secondary="blockuncle",
        order_by=BlockUncle.idx,
    )

    is_canonical = Column(Boolean, nullable=False)

    _parent_hash = Column(LargeBinary(32), ForeignKey('header.hash'), nullable=True, index=True)
    uncles_hash = Column(LargeBinary(32), nullable=False)
    coinbase = Column(LargeBinary(20), nullable=False)
    state_root = Column(LargeBinary(32), nullable=False)
    transaction_root = Column(LargeBinary(32), nullable=False)
    receipt_root = Column(LargeBinary(32), nullable=False)
    _bloom = Column(LargeBinary(1024), nullable=False)
    difficulty = Column(LargeBinary(32), nullable=False)
    block_number = Column(BigInteger, index=True, nullable=False)
    gas_limit = Column(BigInteger, nullable=False)
    gas_used = Column(BigInteger, nullable=False)
    timestamp = Column(Integer, nullable=False)
    extra_data = Column(LargeBinary, nullable=False)
    # mix_hash = Column(LargeBinary(32), nullable=False)
    nonce = Column(LargeBinary(8), nullable=False)

    children = relationship("Header", backref=backref("parent", remote_side=[hash]))  # type: ignore

    @property
    def parent_hash(self) -> Optional[Hash32]:
        if self._parent_hash is None:
            if self.block_number == 0:
                return GENESIS_PARENT_HASH
            else:
                return None
        else:
            return Hash32(self._parent_hash)

    @parent_hash.setter
    def parent_hash(self, value: Optional[Hash32]) -> None:
        if value == GENESIS_PARENT_HASH and self.block_number == 0:
            self._parent_hash = None
        else:
            self._parent_hash = value

    @classmethod
    def from_ir(cls, header: HeaderIR) -> 'Header':
        return cls(
            hash=header.hash,
            is_canonical=header.is_canonical,
            _parent_hash=None if header.is_genesis else header.parent_hash,
            uncles_hash=header.uncles_hash,
            coinbase=header.coinbase,
            state_root=header.state_root,
            transaction_root=header.transaction_root,
            receipt_root=header.receipt_root,
            _bloom=header.bloom,
            difficulty=header.difficulty,
            block_number=header.block_number,
            gas_limit=header.gas_limit,
            gas_used=header.gas_used,
            timestamp=header.timestamp,
            extra_data=header.extra_data,
            # mix_hash=header.mix_hash,
            nonce=header.nonce,
        )


class BlockTransaction(Base):
    query = Session.query_property()

    __tablename__ = 'blocktransaction'
    __table_args__ = (
        Index("ix_blocktransaction_idx_block_header_hash", 'idx', 'block_header_hash', unique=True),
        Index(
            "ix_block_header_hash_transaction_hash",
            'block_header_hash',
            'transaction_hash',
            unique=True,
        ),
    )
    idx = Column(Integer, nullable=False)

    block_header_hash = Column(LargeBinary(32), ForeignKey('block.header_hash'), primary_key=True)
    transaction_hash = Column(LargeBinary(32), ForeignKey('transaction.hash'), primary_key=True)

    block = relationship("Block")
    transaction = relationship("Transaction")


class Block(Base):
    query = Session.query_property()

    __tablename__ = 'block'

    header_hash = Column(LargeBinary(32), ForeignKey('header.hash'), primary_key=True)
    header = relationship("Header", back_populates="block")

    uncles = relationship(
        "Header",
        secondary="blockuncle",
        order_by=BlockUncle.idx,
    )
    transactions = relationship(
        "Transaction",
        secondary="blocktransaction",
        order_by=BlockTransaction.idx,
    )


class Transaction(Base):
    query = Session.query_property()

    __tablename__ = 'transaction'

    hash = Column(LargeBinary(32), primary_key=True)

    block_header_hash = Column(
        LargeBinary(32),
        ForeignKey("block.header_hash"),
        nullable=True,
        index=True,
    )
    block = relationship("Block")

    blocks = relationship(
        "Block",
        secondary="blocktransaction",
        order_by=BlockTransaction.idx,
    )
    receipt = relationship('Receipt', uselist=False, back_populates="transaction")

    nonce = Column(BigInteger, nullable=False)
    gas_price = Column(BigInteger, nullable=False)
    gas = Column(BigInteger, nullable=False)
    to = Column(LargeBinary(20), nullable=False)
    value = Column(LargeBinary(32), nullable=False)
    data = Column(LargeBinary, nullable=False)
    v = Column(LargeBinary(32), nullable=False)
    r = Column(LargeBinary(32), nullable=False)
    s = Column(LargeBinary(32), nullable=False)

    sender = Column(LargeBinary(20), nullable=False)

    @classmethod
    def from_ir(cls,
                transaction_ir: TransactionIR,
                block_header_hash: Optional[Hash32]) -> 'Transaction':
        return cls(
            hash=transaction_ir.hash,
            block_header_hash=block_header_hash,
            nonce=transaction_ir.nonce,
            gas_price=transaction_ir.gas_price,
            gas=transaction_ir.gas,
            to=transaction_ir.to,
            value=transaction_ir.value,
            data=transaction_ir.data,
            v=transaction_ir.v,
            r=transaction_ir.r,
            s=transaction_ir.s,
            sender=transaction_ir.sender,
        )


class Receipt(Base):
    query = Session.query_property()

    __tablename__ = 'receipt'

    transaction_hash = Column(LargeBinary(32), ForeignKey("transaction.hash"), primary_key=True)
    transaction = relationship('Transaction', back_populates="receipt")

    state_root = Column(LargeBinary(32), nullable=False)
    gas_used = Column(BigInteger, nullable=False)
    _bloom = Column(LargeBinary(1024), nullable=False)
    logs = relationship("Log", back_populates="receipt", order_by='Log.idx')

    @property
    def bloom(self) -> int:
        return big_endian_to_int(self._bloom)

    @bloom.setter
    def bloom(self, value: int) -> None:
        self._bloom = int_to_big_endian(value)

    @classmethod
    def from_ir(cls, receipt_ir: ReceiptIR, transaction_hash: Hash32) -> 'Receipt':
        return cls(
            transaction_hash=transaction_hash,
            state_root=receipt_ir.state_root,
            gas_used=receipt_ir.gas_used,
            _bloom=receipt_ir.bloom,
        )


class LogTopic(Base):
    query = Session.query_property()

    __tablename__ = 'logtopic'
    __table_args__ = (
        UniqueConstraint('idx', 'log_id', name='ix_idx_log_id'),
        Index("ix_idx_topic_topic_log_id", "idx", "topic_topic", "log_id"),
    )
    id = Column(Integer, primary_key=True)

    idx = Column(Integer, nullable=False)

    topic_topic = Column(LargeBinary(32), ForeignKey('topic.topic'), index=True, nullable=False)
    log_id = Column(Integer, ForeignKey('log.id'), index=True, nullable=False)

    topic = relationship('Topic')
    log = relationship('Log')


class Log(Base):
    query = Session.query_property()

    __tablename__ = 'log'
    __table_args__ = (
        UniqueConstraint('idx', 'receipt_hash', name="ix_idx_receipt_hash"),
    )

    id = Column(Integer, primary_key=True)
    idx = Column(Integer, nullable=False)

    receipt_hash = Column(
        LargeBinary(32),
        ForeignKey('receipt.transaction_hash'),
        index=True,
        nullable=False,
    )
    receipt = relationship("Receipt", back_populates="logs")

    address = Column(LargeBinary(20), index=True, nullable=False)
    topics = relationship(
        "Topic",
        secondary="logtopic",
        order_by=LogTopic.idx,
    )
    data = Column(LargeBinary, nullable=False)

    def __repr__(self) -> str:
        return (
            f"Log("
            f"idx={self.idx!r}, "
            f"receipt_hash={self.receipt_hash!r}, "
            f"address={self.address!r}, "
            f"data={self.data!r}, "
            f"topics={self.topics!r}"
            f")"
        )

    def __str__(self) -> str:
        # TODO: use eth_utils.humanize_bytes once it is released
        if len(self.data) > 4:
            pretty_data = humanize_hash(Hash32(self.data))
        else:
            pretty_data = self.data.hex()

        if len(self.topics) == 0:  # type: ignore
            pretty_topics = '(anonymous)'
        else:
            pretty_topics = '|'.join((
                humanize_hash(Hash32(topic.topic))
                for topic in self.topics  # type: ignore
            ))

        return f"Log[#{self.idx} A={humanize_hash(self.address)} D={pretty_data}/T={pretty_topics}]"  # type: ignore  # noqa: E501

    @classmethod
    def from_ir(cls, log_ir: LogIR, idx: int, receipt_hash: Hash32) -> 'Log':
        return cls(
            idx=idx,
            receipt_hash=receipt_hash,
            address=log_ir.address,
            data=log_ir.data,
        )


class Topic(Base):
    query = Session.query_property()

    __tablename__ = 'topic'

    topic = Column(LargeBinary(32), primary_key=True)

    logs = relationship(
        "Log",
        secondary="logtopic",
        order_by=LogTopic.idx,
    )

    def __repr__(self) -> str:
        return f"Topic(topic={self.topic!r})"

    def __str__(self) -> str:
        return f"Topic[{humanize_hash(self.topic)}]"  # type: ignore


def query_row_count(session: orm.Session, start_at: int, end_at: int) -> int:
    num_headers = Header.query.filter(
        Header.block_number > start_at,
        Header.block_number <= end_at,
        Header.is_canonical.is_(True),  # type: ignore
    ).count()

    num_uncles = BlockUncle.query.join(
        Block,
        BlockUncle.block_header_hash == Block.header_hash,
    ).join(
        Header,
        Block.header_hash == Header.hash,
    ).filter(
        Header.block_number > start_at,
        Header.block_number <= end_at,
    ).count()

    num_transactions = Transaction.query.join(
        Block,
        Transaction.block_header_hash == Block.header_hash,
    ).join(
        Header,
        Block.header_hash == Header.hash,
    ).filter(
        Header.block_number > start_at,
        Header.block_number <= end_at,
    ).count()

    num_logs = Log.query.join(
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
    ).filter(
        Header.block_number > start_at,
        Header.block_number <= end_at,
    ).count()

    num_topics = LogTopic.query.join(
        Log,
        LogTopic.log_id == Log.id,
    ).join(
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
    ).filter(
        Header.block_number > start_at,
        Header.block_number <= end_at,
    ).count()

    total_item_count = sum((
        num_headers * 2,  # double to account for blocks
        num_uncles * 2,  # double to account for join table
        num_transactions * 3,  # double to account for join table and receipts
        num_logs * 2,  # double to account for join table
        num_topics,  # ignore the topics themselves and only count the join rows
    ))
    return total_item_count
