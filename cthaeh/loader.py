from async_service import Service
import logging
from typing import Iterator, Sequence

from eth_typing import Hash32
from eth_utils import to_tuple
from sqlalchemy import orm
from sqlalchemy.orm.exc import NoResultFound
import trio

from cthaeh.ir import Block as BlockIR
from cthaeh.models import (
    Header,
    Block,
    LogTopic,
    Transaction,
    Receipt,
    Log,
    BlockUncle,
    BlockTransaction,
    Topic,
)


@to_tuple
def get_or_create_topics(session: orm.Session,
                         topics: Sequence[Hash32],
                         ) -> Iterator[Topic]:
    cache: Dict[Hash32, Topic] = {}

    for topic in topics:
        if topic not in cache:
            try:
                cache[topic] = session.query(Topic).filter(Topic.topic == topic).one()
            except NoResultFound:
                cache[topic] = Topic(topic=topic)
        yield cache[topic]


def import_block(session: orm.Session, block_ir: BlockIR) -> None:
    header = Header.from_ir(block_ir.header)
    transactions = tuple(
        Transaction.from_ir(transaction_ir, block_header_hash=header.hash)
        for transaction_ir in block_ir.transactions
    )
    uncles = tuple(
        Header.from_ir(uncle_ir)
        for uncle_ir in block_ir.uncles
    )
    receipts = tuple(
        Receipt.from_ir(receipt_ir, transaction.hash)
        for transaction, receipt_ir
        in zip(transactions, block_ir.receipts)
    )
    log_bundles = tuple(
        tuple(
            Log.from_ir(log_ir, idx, receipt.transaction_hash)
            for idx, log_ir in enumerate(receipt_ir.logs)
        )
        for receipt, receipt_ir in zip(receipts, block_ir.receipts)
    )
    all_logs = tuple(
        log
        for bundle in log_bundles
        for log in bundle
    )
    block = Block(
        header_hash=header.hash,
    )
    block_uncles = tuple(
        BlockUncle(idx=idx, block_header_hash=block.header_hash, uncle_hash=uncle.hash)
        for idx, uncle in enumerate(uncles)
    )
    block_transactions = tuple(
        BlockTransaction(
            idx=idx,
            block_header_hash=block.header_hash,
            transaction_hash=transaction.hash,
        )
        for idx, transaction
        in enumerate(transactions)
    )
    # These need to be lazily created.
    topic_values = tuple(
        topic
        for receipt_ir in block_ir.receipts
        for log_ir in receipt_ir.logs
        for topic in log_ir.topics
    )
    topics = get_or_create_topics(session, topic_values)
    log_topics = tuple(
        LogTopic(idx=idx, topic_topic=topic, log=log)
        for bundle, receipt_ir in zip(log_bundles, block_ir.receipts)
        for log, log_ir in zip(bundle, receipt_ir.logs)
        for idx, topic in enumerate(log_ir.topics)
    )
    session.add(header)
    session.add(block)
    session.add_all(uncles)
    session.add_all(transactions)
    session.add_all(receipts)
    session.add_all(all_logs)
    session.add_all(block_uncles)
    session.add_all(block_transactions)
    session.add_all(topics)
    session.add_all(log_topics)

    session.commit()

    return block


class BlockLoader(Service):
    logger = logging.getLogger('cthaeh.import.BlockLoader')

    def __init__(self,
                 session: orm.Session,
                 block_receive_channel: trio.abc.ReceiveChannel[BlockIR],
                 ) -> None:
        self._block_receive_channel = block_receive_channel
        self._session = session

    async def run(self):
        self.logger.info("Started BlockLoader")

        async with self._block_receive_channel:
            async for block_ir in self._block_receive_channel:
                self.logger.debug("Importing block #%d", block_ir.header.block_number)
                # block = await trio.to_thread.run_sync(import_block, self._session, block_ir)
                block = import_block(self._session, block_ir)
                self.logger.info("Imported block #%d", block.header.block_number)
