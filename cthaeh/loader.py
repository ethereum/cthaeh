import itertools
import logging
import time
from typing import Iterator, Optional, Sequence

from async_service import Service
from eth_typing import BlockNumber, Hash32
from eth_utils import humanize_hash, to_tuple
from sqlalchemy import or_, orm
from sqlalchemy.orm import aliased
from sqlalchemy.orm.exc import NoResultFound
import trio
from web3 import Web3

from cthaeh._utils import every
from cthaeh.ema import EMA
from cthaeh.exceptions import NoGapFound
from cthaeh.exfiltration import HistoryExfiltrator
from cthaeh.ir import Block as BlockIR
from cthaeh.ir import HeadBlockPacket
from cthaeh.ir import Header as HeaderIR
from cthaeh.ir import Transaction as TransactionIR
from cthaeh.lru import LRU
from cthaeh.models import (
    Block,
    BlockTransaction,
    BlockUncle,
    Header,
    Log,
    LogTopic,
    Receipt,
    Topic,
    Transaction,
    query_row_count,
)

logger = logging.getLogger("cthaeh.import")


@to_tuple
def get_or_create_topics(
    session: orm.Session, topics: Sequence[Hash32], cache: LRU[Hash32, None]
) -> Iterator[Topic]:
    for topic in topics:
        if topic not in cache:
            try:
                session.query(Topic).filter(Topic.topic == topic).one()  # type: ignore
            except NoResultFound:
                cache[topic] = None
                yield Topic(topic=topic)


@to_tuple
def get_or_create_transactions(
    session: orm.Session,
    transactions: Sequence[TransactionIR],
    block_header_hash: Hash32,
) -> Iterator[Transaction]:
    transaction_hashes = set(transaction_ir.hash for transaction_ir in transactions)
    already_present_hashes = set(
        result.hash
        for result in session.query(Transaction.hash)  # type: ignore
        .filter(Transaction.hash.in_(transaction_hashes))
        .all()
    )
    session.query(Transaction).filter(  # type: ignore
        Transaction.hash.in_(already_present_hashes)
    ).update({"block_header_hash": block_header_hash}, synchronize_session=False)
    for transaction_ir in transactions:
        if transaction_ir.hash in already_present_hashes:
            continue
        yield Transaction.from_ir(transaction_ir, block_header_hash=block_header_hash)


@to_tuple
def get_or_create_uncles(
    session: orm.Session, uncles: Sequence[HeaderIR]
) -> Iterator[Header]:
    uncle_hashes = set(uncle_ir.hash for uncle_ir in uncles)
    already_present_hashes = set(
        result.hash
        for result in session.query(Header.hash)  # type: ignore
        .filter(Header.hash.in_(uncle_hashes))
        .all()
    )
    session.query(Header).filter(  # type: ignore
        Header.hash.in_(already_present_hashes),
        Header.is_canonical.is_(True),  # type: ignore
    ).update({"is_canonical": False}, synchronize_session=False)
    for uncle_ir in uncles:
        if uncle_ir.hash in already_present_hashes:
            continue
        yield Header.from_ir(uncle_ir)


@to_tuple
def get_or_create_blocktransactions(
    session: orm.Session,
    transactions: Sequence[TransactionIR],
    block_header_hash: Hash32,
) -> Iterator[BlockTransaction]:
    transaction_hashes = set(transaction_ir.hash for transaction_ir in transactions)
    already_present_hashes = set(
        result.transaction_hash
        for result in session.query(  # type: ignore
            BlockTransaction.transaction_hash
        ).filter(
            BlockTransaction.block_header_hash == block_header_hash,
            BlockTransaction.transaction_hash.in_(transaction_hashes),
        )
    )
    for idx, transaction_ir in enumerate(transactions):
        if transaction_ir.hash in already_present_hashes:
            continue
        yield BlockTransaction(
            idx=idx,
            transaction_hash=transaction_ir.hash,
            block_header_hash=block_header_hash,
        )


@to_tuple
def get_or_create_blockuncles(
    session: orm.Session, uncles: Sequence[HeaderIR], block_header_hash: Hash32
) -> Iterator[BlockUncle]:
    uncle_hashes = set(uncle_ir.hash for uncle_ir in uncles)
    already_present_hashes = set(
        result.hash
        for result in session.query(BlockUncle.uncle_hash)  # type: ignore
        .filter(
            BlockUncle.block_header_hash == block_header_hash,
            BlockUncle.uncle_hash.in_(uncle_hashes),
        )
        .all()
    )
    for idx, uncle_ir in enumerate(uncles):
        if uncle_ir.hash in already_present_hashes:
            continue
        yield BlockUncle(
            idx=idx, block_header_hash=block_header_hash, uncle_hash=uncle_ir.hash
        )


def import_block(
    session: orm.Session,
    block_ir: BlockIR,
    cache: LRU[Hash32, None],
    is_detatched: bool = False,
) -> None:
    with session.begin_nested():
        # TODO: Things that need to be cleaned up here.
        # 1. get_or_create_uncles and get_or_create_transactions end up doing one
        # query per object.  They can be optimized to only perform a single query
        # no matter how many objects are present.
        #
        # 2. the cache is being re-used across transactions/topics/uncles.  we
        # should be using a separate cache for each.
        try:
            header = (
                session.query(Header)  # type: ignore
                .filter(Header.hash == block_ir.header.hash)
                .one()
            )
        except NoResultFound:
            header = Header.from_ir(block_ir.header, is_detatched=is_detatched)
            block = Block(header_hash=header.hash)
            session.add_all((header, block))  # type: ignore
        else:
            header.is_canonical = block_ir.header.is_canonical
            block = header.block

        new_transactions = get_or_create_transactions(
            session, block_ir.transactions, block_ir.header.hash
        )

        # Link the uncles to block
        new_blocktransactions = get_or_create_blocktransactions(
            session, block_ir.transactions, block_ir.header.hash
        )

        # Note: this only contains new uncles.
        new_uncles = get_or_create_uncles(session, block_ir.uncles)

        # Link the uncles to block
        blockuncles = get_or_create_blockuncles(
            session, block_ir.uncles, block_ir.header.hash
        )

        receipts = tuple(
            Receipt.from_ir(receipt_ir, Hash32(transaction_ir.hash))
            for transaction_ir, receipt_ir in zip(
                block_ir.transactions, block_ir.receipts
            )
        )

        log_bundles = tuple(
            tuple(
                Log.from_ir(log_ir, idx, Hash32(receipt.transaction_hash))
                for idx, log_ir in enumerate(receipt_ir.logs)
            )
            for receipt, receipt_ir in zip(receipts, block_ir.receipts)
        )
        logs = tuple(itertools.chain(*log_bundles))

        # These need to be lazily created.
        topic_values = tuple(
            topic
            for receipt_ir in block_ir.receipts
            for log_ir in receipt_ir.logs
            for topic in log_ir.topics
        )
        new_topics = get_or_create_topics(session, topic_values, cache)
        logtopics = tuple(
            LogTopic(
                idx=idx,
                topic_topic=topic,
                log_receipt_hash=log.receipt_hash,
                log_idx=log.idx,
            )
            for bundle, receipt_ir in zip(log_bundles, block_ir.receipts)
            for log, log_ir in zip(bundle, receipt_ir.logs)
            for idx, topic in enumerate(log_ir.topics)
        )

        objects_to_save = tuple(
            itertools.chain(
                new_uncles,
                new_transactions,
                receipts,
                logs,
                blockuncles,
                new_blocktransactions,
                new_topics,
                logtopics,
            )
        )
        session.add_all(objects_to_save)  # type: ignore


def orphan_header_chain(session: orm.Session, orphans: Sequence[HeaderIR]) -> None:
    with session.begin_nested():
        header_hashes = set(header_ir.hash for header_ir in orphans)

        # Set all the now orphaned headers as being non-canonical
        session.query(Header).filter(  # type: ignore
            Header.hash.in_(header_hashes)
        ).update({"is_canonical": False}, synchronize_session=False)

        # Unlink each transaction from the block.  We query across the
        # `BlockTransaction` join table because the
        # `Transaction.block_header_hash` field may have already been set to
        # null in the case that this transaction has already been part of
        # another re-org.

        # We can't perform an `.update()` call if we do this with a join so first
        # we pull the transaction hashes above and then we execute the update.
        transactions = (
            session.query(Transaction)  # type: ignore
            .join(
                BlockTransaction, Transaction.hash == BlockTransaction.transaction_hash
            )
            .filter(
                or_(
                    BlockTransaction.block_header_hash.in_(header_hashes),
                    Transaction.block_header_hash.in_(header_hashes),
                )
            )
            .all()
        )

        if not transactions:
            logger.debug("No orphaned transactions to unlink....")

        for transaction in transactions:
            logger.debug(
                "Unlinking txn: %s from block %s",
                humanize_hash(transaction.hash),
                humanize_hash(transaction.block_header_hash),
            )
            transaction.block_header_hash = None

            if transaction.receipt is not None:
                for log_idx, log in enumerate(transaction.receipt.logs):
                    logger.debug("Deleting log #%d", log_idx)
                    for logtopic in log.logtopics:
                        logger.debug(
                            "Deleting logtopic #%d: %s",
                            logtopic.idx,
                            humanize_hash(logtopic.topic_topic),
                        )
                        with session.begin_nested():
                            session.delete(logtopic)  # type: ignore
                    with session.begin_nested():
                        session.delete(log)  # type: ignore
                logger.debug(
                    "Deleting txn receipt: %s", humanize_hash(transaction.hash)
                )
                with session.begin_nested():
                    session.delete(transaction.receipt)  # type: ignore
            else:
                logger.debug("Txn %s already has null receipt")


class HeadLoader(Service):
    logger = logging.getLogger("cthaeh.import.HeadLoader")

    def __init__(
        self,
        session: orm.Session,
        block_receive_channel: trio.abc.ReceiveChannel[HeadBlockPacket],
    ) -> None:
        self._block_receive_channel = block_receive_channel
        self._session = session
        self._topic_cache: LRU[Hash32, None] = LRU(100_000)

    async def _handle_head_packet(
        self, packet: HeadBlockPacket, is_detatched: bool = False
    ) -> None:
        self.logger.debug(
            "Importing new HEAD #%d", packet.blocks[0].header.block_number
        )
        with self._session.begin_nested():
            if packet.orphans:
                self.logger.info(
                    "Reorg orphaned %d blocks starting at %s",
                    len(packet.orphans),
                    packet.orphans[0],
                )
                orphan_header_chain(self._session, packet.orphans)

            import_block(
                self._session,
                packet.blocks[0],
                self._topic_cache,
                is_detatched=is_detatched,
            )

            for block_ir in packet.blocks[1:]:
                import_block(self._session, block_ir, self._topic_cache)

        self._session.commit()  # type: ignore

        self.logger.info("New chain HEAD: #%s", packet.blocks[-1].header)

    async def run(self) -> None:
        self.logger.info("Started BlockLoader")

        async with self._block_receive_channel:
            first_packet = await self._block_receive_channel.receive()
            if first_packet.blocks[0].header.block_number > 0:
                is_detatched = (
                    self._session.query(Header)  # type: ignore
                    .filter(Header.hash == first_packet.blocks[0].header.parent_hash)
                    .scalar()
                    is None
                )
            else:
                is_detatched = False

            await self._handle_head_packet(first_packet, is_detatched=is_detatched)

            async for packet in self._block_receive_channel:
                await self._handle_head_packet(packet)


def find_first_missing_block_number(session: orm.Session) -> BlockNumber:
    child_header = aliased(Header)
    history_head = (
        session.query(Header)  # type: ignore
        .outerjoin(child_header, Header.hash == child_header._parent_hash)
        .filter(
            Header.is_canonical.is_(True),  # type: ignore
            child_header._parent_hash.is_(None),
        )
        .order_by(Header.block_number)
        .first()
    )

    if history_head is None:
        return BlockNumber(0)
    else:
        return BlockNumber(history_head.block_number + 1)


def find_first_detatched_block_after(
    session: orm.Session, after_height: BlockNumber
) -> BlockNumber:
    gap_upper_bound = (
        session.query(Header)  # type: ignore
        .filter(
            Header.is_canonical.is_(True),  # type: ignore
            Header._parent_hash.is_(None),  # type: ignore
            Header._detatched_parent_hash.isnot(None),  # type: ignore
            Header.block_number > after_height,
        )
        .order_by(Header.block_number)
        .first()
    )
    if gap_upper_bound is not None:
        return BlockNumber(gap_upper_bound.block_number)
    else:
        raise NoGapFound(f"No detatched blocks found after height #{after_height}")


class HistoryFiller(Service):
    logger = logging.getLogger("cthaeh.loader.HistoryFiller")

    def __init__(
        self,
        session: orm.Session,
        w3: Web3,
        concurrency: int,
        start_height: Optional[BlockNumber],
        end_height: BlockNumber,
    ) -> None:
        self._w3 = w3
        self._session = session
        self._start_height = start_height
        self._end_height = end_height
        self._concurrency_factor = concurrency

    async def run(self) -> None:
        while True:
            start_height = find_first_missing_block_number(self._session)
            if self._start_height is not None and start_height < self._start_height:
                is_detatched = True
                start_height = self._start_height
            else:
                is_detatched = False

            if start_height >= self._end_height:
                self.logger.info("No gaps found in header chain")
                break

            try:
                end_height = min(
                    self._end_height,
                    find_first_detatched_block_after(self._session, start_height),
                )
            except NoGapFound:
                end_height = self._end_height

            self.logger.info("Gap identified: %d - %d", start_height, end_height)

            send_channel, receive_channel = trio.open_memory_channel[BlockIR](128)

            exfiltrator = HistoryExfiltrator(
                w3=self._w3,
                start_at=start_height,
                end_at=end_height,
                block_send_channel=send_channel,
                concurrency_factor=self._concurrency_factor,
            )
            loader = HistoryLoader(
                session=self._session,
                block_receive_channel=receive_channel,
                is_detatched=is_detatched,
            )

            self.logger.info(
                "Running history import: #%d -> %d", start_height, end_height
            )

            exfiltrator_manager = self.manager.run_child_service(exfiltrator)
            loader_manager = self.manager.run_child_service(loader)

            await exfiltrator_manager.wait_finished()
            await loader_manager.wait_finished()

            await trio.sleep(
                0.05
            )  # small sleep to ensure we aren't in the process of cancellation
            if exfiltrator_manager.did_error or loader_manager.did_error:
                self.logger.error(
                    "Error during import import: #%d -> %d", start_height, end_height
                )
                break
            else:
                self.logger.info(
                    "Finished history import: #%d -> %d", start_height, end_height
                )

            # TODO: connect disconnected head to the new present head
            try:
                detatched_header = (
                    self._session.query(Header)  # type: ignore
                    .filter(
                        Header.is_canonical.is_(True),  # type: ignore
                        Header.block_number == end_height,
                    )
                    .one()
                )
            except NoResultFound:
                self.logger.info("Finished full history import")
                self.manager.cancel()
                break

            with self._session.begin_nested():
                detatched_header._parent_hash = detatched_header._detatched_parent_hash
                detatched_header._detatched_parent_hash = None
                self.logger.info(
                    "Joined detatched header to main chain: #%d (%s)",
                    detatched_header.block_number,
                    humanize_hash(detatched_header.hash),
                )
                self._session.add(detatched_header)


class HistoryLoader(Service):
    logger = logging.getLogger("cthaeh.import.HistoryLoader")

    _last_loaded_block: Optional[BlockIR] = None

    def __init__(
        self,
        session: orm.Session,
        block_receive_channel: trio.abc.ReceiveChannel[BlockIR],
        is_detatched: bool,
    ) -> None:
        self._block_receive_channel = block_receive_channel
        self._session = session
        self._is_detatched = is_detatched
        # TODO: use a shared cache
        self._topic_cache: LRU[Hash32, None] = LRU(100_000)

    async def _handle_import_block(
        self, block_ir: BlockIR, is_detatched: bool = False
    ) -> None:
        self.logger.debug("Importing block #%d", block_ir.header.block_number)
        with self._session.begin_nested():
            import_block(
                self._session, block_ir, self._topic_cache, is_detatched=is_detatched
            )
        self._session.commit()  # type: ignore
        self._last_loaded_block = block_ir

        self.logger.debug("Imported block #%d", block_ir.header.block_number)

    async def run(self) -> None:
        self.logger.info("Started BlockLoader")

        self.manager.run_daemon_task(self._periodically_report_import)

        async with self._block_receive_channel:
            first_block_ir = await self._block_receive_channel.receive()
            await self._handle_import_block(
                first_block_ir, is_detatched=self._is_detatched
            )

            async for block_ir in self._block_receive_channel:
                await self._handle_import_block(block_ir)

        self.manager.cancel()

    async def _periodically_report_import(self) -> None:
        last_reported_height = None
        last_reported_at = None

        import_rate_ema = None

        while self.manager.is_running:
            async for _ in every(5, initial_delay=2):  # noqa: F841
                if self._last_loaded_block is None:
                    self.logger.info("Waiting for first block to load...")
                    continue

                last_loaded_block = self._last_loaded_block
                last_loaded_height = last_loaded_block.header.block_number

                # If this is our *first* report
                if last_reported_height is None or last_reported_at is None:
                    last_reported_height = last_loaded_height
                    last_reported_at = time.monotonic()
                    continue

                if last_loaded_height < last_reported_height:
                    raise Exception("Invariant")
                elif last_loaded_height == last_reported_height:
                    continue

                num_imported = last_loaded_height - last_reported_height
                total_rows = query_row_count(
                    self._session, last_reported_height, last_loaded_height
                )
                duration = time.monotonic() - last_reported_at
                blocks_per_second = num_imported / duration
                items_per_second = total_rows / duration

                if import_rate_ema is None:
                    import_rate_ema = EMA(blocks_per_second, 0.05)
                    items_rate_ema = EMA(items_per_second, 0.05)
                else:
                    import_rate_ema.update(blocks_per_second)
                    items_rate_ema.update(items_per_second)

                self.logger.info(
                    "head=%d (%s) blocks=%d rows=%d bps=%s bps_ema=%s ips=%s, ips_ema=%s",
                    last_loaded_height,
                    humanize_hash(last_loaded_block.header.hash),
                    num_imported,
                    total_rows,
                    (
                        int(blocks_per_second)
                        if blocks_per_second > 2
                        else f"{blocks_per_second:.2f}"
                    ),
                    (
                        int(import_rate_ema.value)
                        if import_rate_ema.value > 2
                        else f"{import_rate_ema.value:.2f}"
                    ),
                    (
                        int(items_per_second)
                        if items_per_second > 2
                        else f"{items_per_second:.2f}"
                    ),
                    (
                        int(items_rate_ema.value)
                        if items_rate_ema.value > 2
                        else f"{items_rate_ema.value:.2f}"
                    ),
                )

                last_reported_height = last_loaded_height
                last_reported_at = time.monotonic()
