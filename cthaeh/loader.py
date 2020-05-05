import itertools
import logging
import time
from typing import Iterator, Optional, Sequence

from async_service import Service
from eth_typing import BlockNumber, Hash32
from eth_utils import humanize_hash, to_tuple
from sqlalchemy import orm
from sqlalchemy.orm import aliased
from sqlalchemy.orm.exc import NoResultFound
import trio
from web3 import Web3

from cthaeh._utils import every
from cthaeh.ema import EMA
from cthaeh.exceptions import NoGapFound
from cthaeh.exfiltration import HistoryExfiltrator
from cthaeh.ir import (
    Block as BlockIR,
    HeadBlockPacket,
    Header as HeaderIR,
    Transaction as TransactionIR,
)
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
from cthaeh.lru import LRU


@to_tuple
def get_or_create_topics(
        session: orm.Session, topics: Sequence[Hash32], cache: LRU[Hash32, None],
) -> Iterator[Topic]:
    for topic in topics:
        if topic not in cache:
            try:
                session.query(Topic).filter(Topic.topic == topic).one()
            except NoResultFound:
                cache[topic] = None
                yield Topic(topic=topic)


@to_tuple
def get_or_create_transactions(
        session: orm.Session,
        header_hash: Hash32,
        transactions: Sequence[TransactionIR],
        cache: LRU[Hash32, None]
) -> Iterator[Transaction]:
    for transaction_ir in transactions:
        if transaction_ir.hash not in cache:
            try:
                transaction = session.query(Transaction).filter(
                    Transaction.hash == transaction_ir.hash,
                ).one()
            except NoResultFound:
                cache[transaction_ir.hash] = None
                yield Transaction.from_ir(transaction_ir, block_header_hash=header_hash)
            else:
                transaction.block_header_hash = header_hash


@to_tuple
def get_or_create_uncles(
        session: orm.Session,
        uncles: Sequence[HeaderIR],
        cache: LRU[Hash32, None],
) -> Iterator[Header]:
    for uncle_ir in uncles:
        if uncle_ir.hash not in cache:
            try:
                session.query(Header).filter(Header.hash == uncle_ir.hash).one()
            except NoResultFound:
                cache[uncle_ir.hash] = None
                yield Header.from_ir(uncle_ir)


def import_block(session: orm.Session,
                 block_ir: BlockIR,
                 cache: LRU[Hash32, None],
                 is_detatched: bool = False) -> None:
    try:
        header = session.query(Header).filter(Header.hash == block_ir.header.hash).one()
        header.is_canonical = True
        if is_detatched:
            header._parent_hash = None
            header._detatched_parent_hash = block_ir.header.parent_hash
        else:
            header._parent_hash = block_ir.header.parent_hash
            header._detatched_parent_hash = None
        block = header.block
    except NoResultFound:
        header = Header.from_ir(block_ir.header, is_detatched=is_detatched)
        block = Block(header_hash=header.hash)

    transaction_hashes = set(
        transaction_ir.hash for transaction_ir in block_ir.transactions
    )
    # Note that `transactions` below is not the full set of transaction
    # objects, but rather only the new ones that were not already
    # present in our database.
    transactions = get_or_create_transactions(
        session=session,
        header_hash=block_ir.header.hash,
        transactions=block_ir.transactions,
        # TODO: need to use a different cache than the topic cache....
        cache=cache,
    )
    block_transactions = tuple(
        BlockTransaction(
            idx=idx,
            block_header_hash=block.header_hash,
            transaction_hash=transaction_hash,
        )
        for idx, transaction_hash in enumerate(transaction_hashes)
    )

    # Note: this only contains new uncles.
    uncles = get_or_create_uncles(session, block_ir.uncles, cache)
    block_uncles = tuple(
        BlockUncle(idx=idx, block_header_hash=block.header_hash, uncle_hash=uncle_ir.hash)
        for idx, uncle_ir in enumerate(block_ir.uncles)
    )

    receipts = tuple(
        Receipt.from_ir(receipt_ir, Hash32(transaction_hash))
        for transaction_hash, receipt_ir in zip(transaction_hashes, block_ir.receipts)
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
        LogTopic(idx=idx, topic_topic=topic, log_receipt_hash=log.receipt_hash, log_idx=log.idx)
        for bundle, receipt_ir in zip(log_bundles, block_ir.receipts)
        for log, log_ir in zip(bundle, receipt_ir.logs)
        for idx, topic in enumerate(log_ir.topics)
    )

    objects_to_save = tuple(
        itertools.chain(
            (header, block),
            uncles,
            transactions,
            receipts,
            logs,
            block_uncles,
            block_transactions,
            new_topics,
            logtopics,
        )
    )
    session.bulk_save_objects(objects_to_save)


def orphan_header_chain(session: orm.Session, orphans: Sequence[HeaderIR]) -> None:
    for header_ir in orphans:
        # Set the header to `is_canonical=False`
        header = session.query(Header).filter(Header.hash == header_ir.hash).one()
        header.is_canonical = False

        # Unlink each transaction from the block
        transactions = session.query(Transaction).join(
            Block,
            Transaction.block_header_hash == Block.header_hash,
        ).filter(
            Block.header_hash == header_ir.hash,
        ).all()

        for transaction in transactions:
            transaction.block_header_hash = None
            session.delete(transaction.receipt)

        objects_to_save = (header,) + tuple(transactions)
        session.bulk_save_objects(objects_to_save)


class HeadLoader(Service):
    logger = logging.getLogger("cthaeh.import.HeadLoader")

    def __init__(
        self,
        session: orm.Session,
        block_receive_channel: trio.abc.ReceiveChannel[HeadBlockPacket],
    ) -> None:
        self._block_receive_channel = block_receive_channel
        self._session = session
        self._topic_cache: LRU[Hash32, None] = LRU(100000)

    async def _handle_head_packet(self,
                                  packet: HeadBlockPacket,
                                  is_detatched: bool = False) -> None:
        self.logger.debug(
            "Importing new HEAD #%d",
            packet.blocks[0].header.block_number,
        )
        if packet.orphans:
            self.logger.info(
                "Reorg orphaned %d blocks starting at %s",
                len(packet.orphans),
                packet.orphans[0],
            )
            with self._session.begin_nested():
                orphan_header_chain(self._session, packet.orphans)

        with self._session.begin_nested():
            import_block(
                self._session,
                packet.blocks[0],
                self._topic_cache,
                is_detatched=is_detatched,
            )

        for block_ir in packet.blocks[1:]:
            with self._session.begin_nested():
                import_block(
                    self._session,
                    block_ir,
                    self._topic_cache,
                )

        self.logger.info(
            "New chain HEAD: #%s", packet.blocks[-1].header
        )

    async def run(self) -> None:
        self.logger.info("Started BlockLoader")

        async with self._block_receive_channel:
            first_packet = await self._block_receive_channel.receive()
            if first_packet.blocks[0].header.block_number > 0:
                is_detatched = self._session.query(Header).filter(
                    Header.is_canonical.is_(True),
                    Header.hash == first_packet.blocks[0].header.parent_hash,
                ).scalar() is None
            else:
                is_detatched = False

            await self._handle_head_packet(first_packet, is_detatched=is_detatched)

            async for packet in self._block_receive_channel:
                await self._handle_head_packet(packet)


def find_first_missing_block_number(session: orm.Session) -> BlockNumber:
    child_header = aliased(Header)
    history_head = session.query(Header).outerjoin(
        child_header,
        Header.hash == child_header._parent_hash,
    ).filter(
        Header.is_canonical.is_(True),
        child_header._parent_hash.is_(None)
    ).order_by(
        Header.block_number,
    ).first()

    if history_head is None:
        return BlockNumber(0)
    else:
        return BlockNumber(history_head.block_number + 1)


def find_first_detatched_block_after(session: orm.Session,
                                     after_height: BlockNumber,
                                     ) -> BlockNumber:
    gap_upper_bound = session.query(Header).filter(
        Header.is_canonical.is_(True),
        Header._parent_hash.is_(None),
        Header._detatched_parent_hash.isnot(None),
        Header.block_number > after_height,
    ).order_by(
        Header.block_number,
    ).first()
    if gap_upper_bound is not None:
        return BlockNumber(gap_upper_bound.block_number)
    else:
        raise NoGapFound(
            f"No detatched blocks found after height #{after_height}"
        )


class HistoryFiller(Service):
    logger = logging.getLogger("cthaeh.loader.HistoryFiller")

    def __init__(self,
                 session: orm.Session,
                 w3: Web3,
                 concurrency: int,
                 start_height: Optional[BlockNumber],
                 end_height: BlockNumber) -> None:
        self._w3 = w3
        self._session = session
        self._start_height = start_height
        self._end_height = end_height
        self._concurrency_factor = concurrency

    async def run(self):
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

            self.logger.info("Running history import: #%d -> %d", start_height, end_height)

            exfiltrator_manager = self.manager.run_child_service(exfiltrator)
            loader_manager = self.manager.run_child_service(loader)

            await exfiltrator_manager.wait_finished()
            self.logger.info("Exfiltrator finished...")
            await loader_manager.wait_finished()
            self.logger.info("Loader finished...")
            if exfiltrator_manager.did_error or loader_manager.did_error:
                self.logger.error("Error during import import: #%d -> %d", start_height, end_height)
                break
            else:
                self.logger.info("Finished history import: #%d -> %d", start_height, end_height)

            # TODO: connect disconnected head to the new present head
            try:
                detatched_header = self._session.query(Header).filter(
                    Header.is_canonical.is_(True),
                    Header.block_number == end_height,
                ).one()
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
        self._commit_lock = trio.Lock()
        self._session = session
        self._is_detatched = is_detatched
        # TODO: use a shared cache
        self._topic_cache: LRU[Hash32, None] = LRU(100000)

    async def _handle_import_block(self, block_ir: BlockIR, is_detatched: bool = False) -> None:
        self.logger.debug(
            "Importing block #%d", block_ir.header.block_number
        )
        async with self._commit_lock:
            import_block(self._session, block_ir, self._topic_cache, is_detatched=is_detatched)
            self._last_loaded_block = block_ir

        self.logger.debug(
            "Imported block #%d", block_ir.header.block_number
        )

    async def run(self) -> None:
        self.logger.info("Started BlockLoader")

        self.manager.run_daemon_task(self._commit_on_interval)
        self.manager.run_daemon_task(self._periodically_report_import)

        async with self._block_receive_channel:
            try:
                first_block_ir = await self._block_receive_channel.receive()
                await self._handle_import_block(first_block_ir, is_detatched=self._is_detatched)

                async for block_ir in self._block_receive_channel:
                    await self._handle_import_block(block_ir)
            finally:
                self._session.commit()  # type: ignore

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

    async def _commit_on_interval(self) -> None:
        async for _ in every(1):  # noqa: F841
            async with self._commit_lock:
                self._session.commit()  # type: ignore
