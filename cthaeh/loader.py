from async_service import Service
import itertools
import logging
import time
from typing import Iterator, Sequence

from eth_typing import Hash32
from eth_utils import to_tuple, humanize_hash
from sqlalchemy import orm
from sqlalchemy.orm.exc import NoResultFound
import trio

from cthaeh._utils import every
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
    logs = tuple(itertools.chain(*log_bundles))
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
    logtopics = tuple(
        LogTopic(idx=idx, topic_topic=topic, log=log)
        for bundle, receipt_ir in zip(log_bundles, block_ir.receipts)
        for log, log_ir in zip(bundle, receipt_ir.logs)
        for idx, topic in enumerate(log_ir.topics)
    )

    objects_to_save = tuple(itertools.chain(
        (header, block),
        uncles,
        transactions,
        receipts,
        logs,
        block_uncles,
        block_transactions,
        topics,
        logtopics,
    ))
    session.bulk_save_objects(objects_to_save)


class BlockLoader(Service):
    logger = logging.getLogger('cthaeh.import.BlockLoader')

    def __init__(self,
                 session: orm.Session,
                 block_receive_channel: trio.abc.ReceiveChannel[BlockIR],
                 ) -> None:
        self._block_receive_channel = block_receive_channel
        self._commit_lock = trio.Lock()
        self._session = session
        self._last_loaded_block = None

    async def run(self):
        self.logger.info("Started BlockLoader")

        self.manager.run_daemon_task(self._commit_on_interval)
        self.manager.run_daemon_task(self._periodically_report_import)

        async with self._block_receive_channel:
            try:
                async for block_ir in self._block_receive_channel:
                    self.logger.debug("Importing block #%d", block_ir.header.block_number)
                    async with self._commit_lock:
                        import_block(self._session, block_ir)
                        self._last_loaded_block = block_ir

                    self.logger.debug("Imported block #%d", block_ir.header.block_number)
            finally:
                self._session.commit()

    async def _periodically_report_import(self) -> None:
        last_reported_height = None
        last_reported_at = None

        while self.manager.is_running:
            async for _ in every(5, initial_delay=2):  # noqa: F841
                last_loaded_block = self._last_loaded_block
                last_loaded_height = last_loaded_block.header.block_number

                if last_loaded_block is None:
                    self.logger.info("Waiting for first block to load...")
                    continue

                # If this is our *first* report
                if last_reported_height is None or last_reported_at is None:
                    last_reported_height = last_loaded_height
                    last_reported_at = time.monotonic()
                    continue

                if last_loaded_height < last_reported_height:
                    raise Exception("Invariant")

                num_imported = last_loaded_height - last_reported_height
                duration = time.monotonic() - last_reported_at
                blocks_per_second = num_imported / duration

                self.logger.info(
                    "head=%d (%s) count=%d bps=%s",
                    last_loaded_height,
                    humanize_hash(last_loaded_block.header.hash),
                    num_imported,
                    (
                        int(blocks_per_second)
                        if blocks_per_second > 2
                        else f"{blocks_per_second:.2f}"
                    ),
                )

                last_reported_height = last_loaded_height
                last_reported_at = time.monotonic()

    async def _commit_on_interval(self) -> None:
        async for _ in every(1):  # noqa: F841
            async with self._commit_lock:
                self._session.commit()
