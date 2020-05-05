from typing import Optional

from eth_typing import Hash32
from eth_utils import big_endian_to_int, humanize_hash, int_to_big_endian
from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    LargeBinary,
    UniqueConstraint,
    and_,
    orm,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import backref, relationship

from cthaeh.constants import GENESIS_PARENT_HASH
from cthaeh.ir import Header as HeaderIR
from cthaeh.ir import Log as LogIR
from cthaeh.ir import Receipt as ReceiptIR
from cthaeh.ir import Transaction as TransactionIR
from cthaeh.session import Session

Base = declarative_base()


class BlockUncle(Base):
    query = Session.query_property()

    __tablename__ = "blockuncle"
    __table_args__ = (
        Index(
            "ix_blockuncle_idx_block_header_hash",
            "idx",
            "block_header_hash",
            unique=True,
        ),
        Index(
            "ix_block_header_hash_uncle_hash",
            "block_header_hash",
            "uncle_hash",
            unique=True,
        ),
    )

    idx = Column(Integer, nullable=False)

    block_header_hash = Column(
        LargeBinary(32), ForeignKey("block.header_hash"), primary_key=True
    )
    uncle_hash = Column(LargeBinary(32), ForeignKey("header.hash"), primary_key=True)

    block = relationship("Block")
    uncle = relationship("Header")


class Header(Base):
    query = Session.query_property()

    __tablename__ = "header"
    __table_args__ = (
        CheckConstraint(
            "_parent_hash is null or _detatched_parent_hash is null",
            name="_no_double_parent_hash",
        ),
    )

    hash = Column(LargeBinary(32), primary_key=True)

    block = relationship("Block", uselist=False, back_populates="header")
    uncle_blocks = relationship(
        "Block", secondary="blockuncle", order_by=BlockUncle.idx
    )

    is_canonical = Column(Boolean, nullable=False)

    _detatched_parent_hash = Column(LargeBinary(32), nullable=True, index=True)
    _parent_hash = Column(
        LargeBinary(32), ForeignKey("header.hash"), nullable=True, index=True
    )
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

    children = relationship(
        "Header", backref=backref("parent", remote_side=[hash])  # type: ignore
    )

    @property
    def is_genesis(self) -> bool:
        return (
            self.block_number == 0
            and self.is_canonical  # noqa: W503
            and self.parent_hash == GENESIS_PARENT_HASH  # noqa: W503
        )

    @property
    def is_detatched(self) -> bool:
        return self._parent_hash is None and self._detatched_parent_hash is not None

    @property
    def parent_hash(self) -> Optional[Hash32]:
        if self._parent_hash is not None and self._detatched_parent_hash is not None:
            raise TypeError("Invalid: header has two parent hashes")
        elif self._detatched_parent_hash is not None:
            return Hash32(self._detatched_parent_hash)
        elif self._parent_hash is None:
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
    def from_ir(cls, header: HeaderIR, is_detatched: bool = False) -> "Header":
        parent_hash: Optional[Hash32]
        if is_detatched or header.is_genesis:
            parent_hash = None
        else:
            parent_hash = header.parent_hash

        detatched_parent_hash: Optional[Hash32]
        if is_detatched:
            detatched_parent_hash = header.parent_hash
        else:
            detatched_parent_hash = None

        return cls(
            hash=header.hash,
            is_canonical=header.is_canonical,
            _parent_hash=parent_hash,
            _detatched_parent_hash=detatched_parent_hash,
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

    __tablename__ = "blocktransaction"
    __table_args__ = (
        Index(
            "ix_blocktransaction_idx_block_header_hash",
            "idx",
            "block_header_hash",
            unique=True,
        ),
        Index(
            "ix_block_header_hash_transaction_hash",
            "block_header_hash",
            "transaction_hash",
            unique=True,
        ),
    )
    idx = Column(Integer, nullable=False)

    block_header_hash = Column(
        LargeBinary(32), ForeignKey("block.header_hash"), primary_key=True
    )
    transaction_hash = Column(
        LargeBinary(32), ForeignKey("transaction.hash"), primary_key=True
    )

    block = relationship("Block")
    transaction = relationship("Transaction")


class Block(Base):
    query = Session.query_property()

    __tablename__ = "block"

    header_hash = Column(LargeBinary(32), ForeignKey("header.hash"), primary_key=True)
    header = relationship("Header", back_populates="block")

    uncles = relationship("Header", secondary="blockuncle", order_by=BlockUncle.idx)
    transactions = relationship(
        "Transaction", secondary="blocktransaction", order_by=BlockTransaction.idx
    )


class Transaction(Base):
    query = Session.query_property()

    __tablename__ = "transaction"

    hash = Column(LargeBinary(32), primary_key=True)

    block_header_hash = Column(
        LargeBinary(32), ForeignKey("block.header_hash"), nullable=True, index=True
    )
    block = relationship("Block")

    blocks = relationship(
        "Block", secondary="blocktransaction", order_by=BlockTransaction.idx
    )
    receipt = relationship(
        "Receipt", uselist=False, back_populates="transaction", cascade="all"
    )

    nonce = Column(BigInteger, nullable=False)
    gas_price = Column(BigInteger, nullable=False)
    gas = Column(BigInteger, nullable=False)
    to = Column(LargeBinary(20), nullable=True)
    value = Column(LargeBinary(32), nullable=False)
    data = Column(LargeBinary, nullable=False)
    v = Column(LargeBinary(32), nullable=False)
    r = Column(LargeBinary(32), nullable=False)
    s = Column(LargeBinary(32), nullable=False)

    sender = Column(LargeBinary(20), nullable=False)

    @classmethod
    def from_ir(
        cls, transaction_ir: TransactionIR, block_header_hash: Optional[Hash32]
    ) -> "Transaction":
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

    __tablename__ = "receipt"

    transaction_hash = Column(
        LargeBinary(32), ForeignKey("transaction.hash"), primary_key=True
    )
    transaction = relationship("Transaction", back_populates="receipt")

    state_root = Column(LargeBinary(32), nullable=False)
    gas_used = Column(BigInteger, nullable=False)
    _bloom = Column(LargeBinary(1024), nullable=False)
    logs = relationship(
        "Log", back_populates="receipt", order_by="Log.idx", cascade="all"
    )

    @property
    def bloom(self) -> int:
        return big_endian_to_int(self._bloom)

    @bloom.setter
    def bloom(self, value: int) -> None:
        self._bloom = int_to_big_endian(value)

    @classmethod
    def from_ir(cls, receipt_ir: ReceiptIR, transaction_hash: Hash32) -> "Receipt":
        return cls(
            transaction_hash=transaction_hash,
            state_root=receipt_ir.state_root,
            gas_used=receipt_ir.gas_used,
            _bloom=receipt_ir.bloom,
        )


class LogTopic(Base):
    query = Session.query_property()

    __tablename__ = "logtopic"
    __table_args__ = (
        UniqueConstraint(
            "idx", "log_receipt_hash", "log_idx", name="ix_idx_log_receipt_hash_log_idx"
        ),
        Index(
            "ix_idx_topic_topic_log_receipt_hash_log_idx",
            "idx",
            "topic_topic",
            "log_receipt_hash",
            "log_idx",
        ),
        ForeignKeyConstraint(
            ("log_idx", "log_receipt_hash"), ("log.idx", "log.receipt_hash")
        ),
    )
    __mapper_args__ = {"confirm_deleted_rows": False}

    id = Column(Integer, primary_key=True)

    idx = Column(Integer, nullable=False)

    topic_topic = Column(
        LargeBinary(32), ForeignKey("topic.topic"), index=True, nullable=False
    )
    log_idx = Column(Integer, nullable=False)
    log_receipt_hash = Column(LargeBinary(32), nullable=False)

    topic = relationship("Topic")
    log = relationship("Log", foreign_keys=[log_idx, log_receipt_hash])


class Log(Base):
    query = Session.query_property()

    __tablename__ = "log"
    __table_args__ = (
        UniqueConstraint("idx", "receipt_hash", name="ix_idx_receipt_hash"),
    )

    # composite primary key
    idx = Column(Integer, primary_key=True, index=True)
    receipt_hash = Column(
        LargeBinary(32),
        ForeignKey("receipt.transaction_hash"),
        primary_key=True,
        index=True,
    )

    receipt = relationship("Receipt", back_populates="logs")

    address = Column(LargeBinary(20), index=True, nullable=False)
    topics = relationship(
        "Topic",
        secondary="logtopic",
        order_by=LogTopic.idx,
        primaryjoin=and_(
            LogTopic.log_idx == idx, LogTopic.log_receipt_hash == receipt_hash
        ),
    )
    logtopics = relationship(
        "LogTopic",
        foreign_keys=(LogTopic.log_idx, LogTopic.log_receipt_hash),
        cascade="all",
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
            pretty_topics = "(anonymous)"
        else:
            pretty_topics = "|".join(
                (
                    humanize_hash(Hash32(topic.topic))
                    for topic in self.topics  # type: ignore
                )
            )

        return f"Log[#{self.idx} A={humanize_hash(self.address)} D={pretty_data}/T={pretty_topics}]"  # type: ignore  # noqa: E501

    @classmethod
    def from_ir(cls, log_ir: LogIR, idx: int, receipt_hash: Hash32) -> "Log":
        return cls(
            idx=idx, receipt_hash=receipt_hash, address=log_ir.address, data=log_ir.data
        )


class Topic(Base):
    query = Session.query_property()

    __tablename__ = "topic"

    topic = Column(LargeBinary(32), primary_key=True)

    logs = relationship(
        "Log",
        secondary="logtopic",
        order_by=LogTopic.idx,
        primaryjoin=(LogTopic.topic_topic == topic),
        secondaryjoin=and_(
            LogTopic.log_idx == Log.idx, LogTopic.log_receipt_hash == Log.receipt_hash
        ),
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

    num_uncles = (
        BlockUncle.query.join(Block, BlockUncle.block_header_hash == Block.header_hash)
        .join(Header, Block.header_hash == Header.hash)
        .filter(Header.block_number > start_at, Header.block_number <= end_at)
        .count()
    )

    num_transactions = (
        Transaction.query.join(
            Block, Transaction.block_header_hash == Block.header_hash
        )
        .join(Header, Block.header_hash == Header.hash)
        .filter(Header.block_number > start_at, Header.block_number <= end_at)
        .count()
    )

    num_logs = (
        Log.query.join(Receipt, Log.receipt_hash == Receipt.transaction_hash)
        .join(Transaction, Receipt.transaction_hash == Transaction.hash)
        .join(Block, Transaction.block_header_hash == Block.header_hash)
        .join(Header, Block.header_hash == Header.hash)
        .filter(Header.block_number > start_at, Header.block_number <= end_at)
        .count()
    )

    num_topics = (
        LogTopic.query.join(Log, LogTopic.log)
        .join(Receipt, Log.receipt)
        .join(Transaction, Receipt.transaction)
        .join(Block, Transaction.block)
        .join(Header, Block.header)
        .filter(Header.block_number > start_at, Header.block_number <= end_at)
        .count()
    )

    total_item_count = sum(
        (
            num_headers * 2,  # double to account for blocks
            num_uncles * 2,  # double to account for join table
            num_transactions * 3,  # double to account for join table and receipts
            num_logs * 2,  # double to account for join table
            num_topics,  # ignore the topics themselves and only count the join rows
        )
    )
    return total_item_count
