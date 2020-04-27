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

    idx = Column(Integer)

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

    is_canonical = Column(Boolean)

    _parent_hash = Column(LargeBinary(32), ForeignKey('header.hash'), nullable=True, index=True)
    uncles_hash = Column(LargeBinary(32))
    coinbase = Column(LargeBinary(20))
    state_root = Column(LargeBinary(32))
    transaction_root = Column(LargeBinary(32))
    receipt_root = Column(LargeBinary(32))
    _bloom = Column(LargeBinary(1024))
    difficulty = Column(LargeBinary(32))
    block_number = Column(BigInteger, index=True)
    gas_limit = Column(BigInteger)
    gas_used = Column(BigInteger)
    timestamp = Column(Integer)
    extra_data = Column(LargeBinary)
    # mix_hash = Column(LargeBinary(32))
    nonce = Column(LargeBinary(8))

    children = relationship("Header", backref=backref("parent", remote_side=[hash]))

    @property
    def parent_hash(self) -> Hash32:
        if self._parent_hash is None:
            return GENESIS_PARENT_HASH
        else:
            return self._parent_hash

    @parent_hash.setter
    def parent_hash(self, value: Hash32) -> None:
        if value == GENESIS_PARENT_HASH:
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
    idx = Column(Integer)

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

    nonce = Column(BigInteger)
    gas_price = Column(BigInteger)
    gas = Column(BigInteger)
    to = Column(LargeBinary(20))
    value = Column(LargeBinary(32))
    data = Column(LargeBinary)
    v = Column(LargeBinary(32))
    r = Column(LargeBinary(32))
    s = Column(LargeBinary(32))

    sender = Column(LargeBinary(20))

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

    state_root = Column(LargeBinary(32))
    gas_used = Column(BigInteger)
    _bloom = Column(LargeBinary(1024))
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

    idx = Column(Integer)

    topic_topic = Column(LargeBinary(32), ForeignKey('topic.topic'), index=True)
    log_id = Column(Integer, ForeignKey('log.id'), index=True)

    topic = relationship('Topic')
    log = relationship('Log')


class Log(Base):
    query = Session.query_property()

    __tablename__ = 'log'
    __table_args__ = (
        UniqueConstraint('idx', 'receipt_hash', name="ix_idx_receipt_hash"),
    )

    id = Column(Integer, primary_key=True)
    idx = Column(Integer)

    receipt_hash = Column(LargeBinary(32), ForeignKey('receipt.transaction_hash'), index=True)
    receipt = relationship("Receipt", back_populates="logs")

    address = Column(LargeBinary(20), index=True)
    topics = relationship(
        "Topic",
        secondary="logtopic",
        order_by=LogTopic.idx,
    )
    data = Column(LargeBinary)

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
        if len(self.data > 4):
            pretty_data = humanize_hash(self.data)
        else:
            pretty_data = self.data.hex()

        if len(self.topics) == 0:
            pretty_topics = '(anonymous)'
        else:
            pretty_topics = '|'.join((
                humanize_hash(topic.topic)
                for topic in self.topics
            ))

        return f"Log[#{self.idx} A={humanize_hash(self.address)} D={pretty_data}/T={pretty_topics}]"

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
        return f"Topic[{humanize_hash(self.topic)}]"
