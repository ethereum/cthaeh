import bisect
import collections
import itertools
import logging
import math
from typing import (
    Callable,
    Collection,
    Deque,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    cast,
)

from async_service import Service
from eth_typing import BlockIdentifier, BlockNumber, Hash32
from eth_utils import to_bytes, to_canonical_address, to_hex, to_int, to_tuple
import trio
from web3 import Web3
from web3.types import BlockData, LogReceipt, TxData, TxReceipt, Uncle
from web3.exceptions import TransactionNotFound

from cthaeh._utils import gather
from cthaeh.ema import EMA
from cthaeh.ir import Block, HeadBlockPacket, Header, Log, Receipt, Transaction


def iter_block_numbers(
    start_at: BlockNumber, end_at: BlockNumber
) -> Iterator[BlockNumber]:
    return (BlockNumber(value) for value in range(start_at, end_at))


class HistoryExfiltrator(Service):
    _concurrency_factor: int
    w3: Web3
    _block_send_channel: trio.abc.SendChannel[Block]
    start_at: BlockNumber
    end_at: BlockNumber

    logger = logging.getLogger("cthaeh.exfiltration.HistoryExfiltrator")

    def __init__(
        self,
        w3: Web3,
        start_at: BlockNumber,
        end_at: BlockNumber,
        block_send_channel: trio.abc.SendChannel[Block],
        concurrency_factor: int,
    ) -> None:
        self.w3 = w3
        self.start_at = start_at
        self.end_at = end_at
        self._block_send_channel = block_send_channel
        self._concurrency_factor = concurrency_factor

    async def run(self) -> None:
        self.logger.info("Started historical sync: #%d -> %d", self.start_at, self.end_at)
        await self._fetch_blocks()
        self.logger.info("Finished historical sync: #%d -> %d", self.start_at, self.end_at)

    async def _fetch_blocks(self) -> None:
        semaphor = trio.Semaphore(
            self._concurrency_factor, max_value=self._concurrency_factor
        )

        (relay_send_channel, relay_receive_channel) = trio.open_memory_channel[Block](0)

        self.manager.run_daemon_task(self._collate_and_relay, relay_receive_channel)

        async def _fetch(block_number: int) -> None:
            # TODO: handle web3 failures
            self.logger.debug("Retrieving block #%d", block_number)
            block = await retrieve_block(self.w3, block_number)
            self.logger.debug("Retrieved block #%d", block_number)
            semaphor.release()
            await relay_send_channel.send(block)

        async with self._block_send_channel:
            async with relay_send_channel:
                async with trio.open_nursery() as nursery:
                    while self.manager.is_running:
                        for block_number in iter_block_numbers(self.start_at, self.end_at):
                            await semaphor.acquire()
                            nursery.start_soon(_fetch, block_number)

    async def _collate_and_relay(self,
                                 receive_channel: trio.abc.ReceiveChannel[Block],
                                 ) -> None:
        buffer: Deque[Block] = collections.deque()
        concurrency_factor = self._concurrency_factor

        warning_threshold = concurrency_factor + max(
            1, int(math.ceil(math.sqrt(concurrency_factor)))
        )
        error_threshold = warning_threshold * 2
        exception_threshold = error_threshold * 2

        next_block_number = self.start_at

        async with receive_channel:
            async with self._block_send_channel:
                async for block in receive_channel:
                    self.logger.debug(
                        "Waiting for #%d, Got block #%d",
                        next_block_number,
                        block.header.block_number,
                    )
                    if block.header.block_number == next_block_number:
                        await self._block_send_channel.send(block)
                        next_block_number += 1  # type: ignore
                    elif block.header.block_number > next_block_number:
                        bisect.insort_left(buffer, block)

                        if len(buffer) > exception_threshold:
                            raise Exception(
                                f"Collation buffer exceeded reasonable limits: "
                                f"concurrency={concurrency_factor} "
                                f"waiting_for={next_block_number} "
                                f"buffer={tuple(blk.header.block_number for blk in buffer)}"
                            )
                        elif len(buffer) > error_threshold:
                            self.logger.error(
                                "Collation buffer exceeded error threshold:"
                                "threshold=%d "
                                "concurrency=%d "
                                "waiting_for=%d "
                                "buffer=%s ",
                                error_threshold,
                                concurrency_factor,
                                next_block_number,
                                tuple(blk.header.block_number for blk in buffer),
                            )
                        elif len(buffer) > warning_threshold:
                            self.logger.warning(
                                "Collation buffer exceeded warning threshold:"
                                "threshold=%d "
                                "concurrency=%d "
                                "waiting_for=%d "
                                "buffer=%s ",
                                error_threshold,
                                concurrency_factor,
                                next_block_number,
                                tuple(blk.header.block_number for blk in buffer),
                            )

                        continue
                    else:
                        raise Exception(
                            f"Expected next block number {next_block_number}.  Got: {block}"
                        )

                    while buffer:
                        if buffer[0].header.block_number == next_block_number:
                            await self._block_send_channel.send(buffer.popleft())
                            next_block_number += 1  # type: ignore
                            continue
                        break


class Poller:
    def __init__(self, start_at: float) -> None:
        self._ema = EMA(start_at, 0.05)
        self._last_at = trio.current_time()

    @property
    def _next_at(self) -> float:
        return self._last_at + self._ema.value

    async def wait(self) -> None:
        now = trio.current_time()

        delta = now - self._next_at
        # before deadline
        if delta < 0:
            await trio.sleep_until(self._next_at)
            return

        # after deadline
        if delta < 3:
            await trio.sleep(2)
        elif delta < 5:
            await trio.sleep(1)
        else:
            await trio.sleep(0.5)

    def update(self) -> None:
        now = trio.current_time()
        delta = now - self._last_at
        self._ema.update(delta)
        self._last_at = now


MAX_REORG_DEPTH = 128


class HeadExfiltrator(Service):
    logger = logging.getLogger("cthaeh.exfiltration.HeadExfiltrator")

    end_at: Optional[BlockNumber]

    def __init__(
        self,
        w3: Web3,
        block_send_channel: trio.abc.SendChannel[HeadBlockPacket],
        start_at: BlockNumber,
        end_at: Optional[BlockNumber],
        concurrency_factor: int,
    ) -> None:
        self.w3 = w3
        self.start_at = start_at
        self.end_at = end_at
        self._block_send_channel = block_send_channel
        self._concurrency_factor = concurrency_factor

    async def run(self) -> None:
        self.manager.run_daemon_task(self._fetch_blocks)
        await self.manager.wait_finished()

    async def _fetch_blocks(self) -> None:
        history: Deque[Header] = collections.deque(maxlen=MAX_REORG_DEPTH)
        history.extend(await retrieve_ancestry(
            self.w3,
            self.start_at,
            until_height_lt(max(0, self.start_at - 128)),
        ))

        first_block = await retrieve_block(self.w3, history[-1].hash)
        self.logger.info("Head exfiltration started: %s", first_block)
        await self._block_send_channel.send(HeadBlockPacket((first_block,)))

        first_timestamp = history[0].timestamp
        last_timestamp = history[-1].timestamp
        if len(history) > 1:
            block_time = (last_timestamp - first_timestamp) / len(history)
        else:
            block_time = 15.0

        poller = Poller(block_time)

        while self.manager.is_running:
            local_head = history[-1]
            chain_head = await retrieve_header(self.w3, 'latest')

            if chain_head < local_head:
                self.logger.warning(
                    "Current head was before last seen head: chain=%s local=%s",
                    chain_head,
                    local_head,
                )
                await trio.sleep(15)
                continue
            elif chain_head >= local_head:
                if chain_head.hash == local_head.hash:
                    self.logger.info("Poll-waiting...")
                    await poller.wait()
                    continue

                # Check for re-org
                history_hashes = set(header.hash for header in history)
                new_headers = await retrieve_ancestry(
                    self.w3,
                    chain_head.hash,
                    until_parent_hash_known(history_hashes, history[0].block_number),
                )
                if not new_headers:
                    raise Exception(f"New Headers empty? %s / %s", new_headers, chain_head)

                insertion_index = bisect.bisect_left(history, new_headers[0])

                orphans = tuple(itertools.islice(history, insertion_index, None))
                if orphans:
                    self.logger.info("Reorg detected: %s", len(orphans))

                try:
                    blocks = await gather(*(
                        (retrieve_block, self.w3, header.hash)
                        for header in new_headers
                    ))
                except TransactionNotFound:
                    continue

                while len(history) > insertion_index:
                    history.pop()
                history.extend(new_headers)

                self.logger.info("New chain head: %s", history[-1])
                await self._block_send_channel.send(HeadBlockPacket(blocks, orphans))
                # Now update the poller for the total amount of new block height
                for _ in range(chain_head.block_number - local_head.block_number):
                    poller.update()
            else:
                raise Exception(f"Unreachable code path: chain={chain_head}  local={local_head}")


def until_height_lt(height: BlockNumber) -> Callable[[Header], bool]:
    def _condition__fn(header: Header) -> bool:
        return header.block_number < height
    return _condition__fn


def until_parent_hash_known(known_parent_hashes: Collection[Hash32],
                            min_height: int,
                            ) -> Callable[[Header], bool]:
    def _condition__fn(header: Header) -> bool:
        if header.block_number < min_height:
            raise Exception("Reached maximum depth")

        return header.hash in known_parent_hashes
    return _condition__fn


async def retrieve_ancestry(w3: Web3,
                            from_height: BlockNumber,
                            until_condition_fn: Callable[[Header], bool],
                            ) -> Tuple[Header, ...]:
    tip = await retrieve_header(w3, from_height)
    history: List[Header] = [tip]
    while True:
        header = await retrieve_header(w3, history[-1].parent_hash)
        if until_condition_fn(header):
            break
        history.append(header)

    return tuple(reversed(history))


async def retrieve_block_number(w3: Web3) -> BlockNumber:
    def _get_block_number():
        return w3.eth.blockNumber

    return await trio.to_thread.run_sync(_get_block_number)


async def retrieve_header(w3: Web3, block_number_or_hash: BlockIdentifier) -> Header:
    block_data = await trio.to_thread.run_sync(w3.eth.getBlock, block_number_or_hash)
    return extract_header(block_data)


async def retrieve_block(w3: Web3, block_number_or_hash: BlockIdentifier) -> Block:
    block_data = await trio.to_thread.run_sync(w3.eth.getBlock, block_number_or_hash, True)
    transactions_data = cast(Sequence[TxData], block_data["transactions"])

    transaction_hashes = tuple(to_hex(tx_data["hash"]) for tx_data in transactions_data)
    receipts_data = await gather(
        *(
            (trio.to_thread.run_sync, w3.eth.getTransactionReceipt, transaction_hash)
            for transaction_hash in transaction_hashes
        )
    )

    uncles_data = await gather(
        *(
            (
                trio.to_thread.run_sync,
                w3.eth.getUncleByBlock,
                to_hex(block_data["hash"]),
                idx,
            )
            for idx in range(len(block_data["uncles"]))
        )
    )

    return extract_block(
        block_data,
        transactions_data=transactions_data,
        uncles_data=uncles_data,
        receipts_data=receipts_data,
    )


def extract_block(
    block_data: BlockData,
    transactions_data: Sequence[TxData],
    uncles_data: Sequence[Uncle],
    receipts_data: Sequence[TxReceipt],
) -> Block:
    header = extract_header(block_data)
    transactions = tuple(extract_transaction(tx_data) for tx_data in transactions_data)
    uncles = tuple(extract_uncle(uncle_data) for uncle_data in uncles_data)
    receipts = tuple(extract_receipt(receipt_data) for receipt_data in receipts_data)
    return Block(
        header=header, transactions=transactions, uncles=uncles, receipts=receipts
    )


def extract_uncle(uncle_data: Uncle) -> Header:
    receipt_root: Hash32

    if "receiptsRoot" in uncle_data:
        receipt_root = Hash32(to_bytes(hexstr=uncle_data["receiptsRoot"]))
    elif "receiptRoot" in uncle_data:
        receipt_root = Hash32(
            to_bytes(hexstr=uncle_data["receiptRoot"])  # type: ignore
        )
    elif "receipts_root" in uncle_data:
        receipt_root = Hash32(
            to_bytes(hexstr=uncle_data["receipts_root"])  # type: ignore
        )
    else:
        raise Exception(f"Cannot find receipts_root key: {uncle_data!r}")

    logs_bloom: bytes

    if "logsBloom" in uncle_data:
        logs_bloom = to_bytes(hexstr=uncle_data["logsBloom"])
    elif "logs_bloom" in uncle_data:
        logs_bloom = to_bytes(hexstr=uncle_data["logs_bloom"])  # type: ignore
    else:
        raise Exception(f"Cannot find logs_bloom key: {uncle_data!r}")

    return Header(
        hash=Hash32(to_bytes(hexstr=uncle_data["hash"])),
        difficulty=Hash32(to_bytes(hexstr=uncle_data["difficulty"])),
        block_number=to_int(hexstr=uncle_data["number"]),
        gas_limit=to_int(hexstr=uncle_data["gasLimit"]),
        timestamp=to_int(hexstr=uncle_data["timestamp"]),
        coinbase=to_canonical_address(uncle_data["miner"]),
        parent_hash=Hash32(to_bytes(hexstr=uncle_data["parentHash"])),
        uncles_hash=Hash32(to_bytes(hexstr=uncle_data["sha3Uncles"])),
        state_root=Hash32(to_bytes(hexstr=uncle_data["stateRoot"])),
        transaction_root=Hash32(to_bytes(hexstr=uncle_data["transactionsRoot"])),
        receipt_root=receipt_root,
        bloom=logs_bloom,
        gas_used=to_int(hexstr=uncle_data["gasUsed"]),
        extra_data=to_bytes(hexstr=uncle_data["extraData"]),
        # mix_hash=Hash32(to_bytes(hexstr=uncle_data['mixHash'])),
        nonce=to_bytes(hexstr=uncle_data["nonce"]),
        is_canonical=False,
    )


def extract_header(block_data: BlockData) -> Header:
    receipt_root: Hash32

    if "receiptsRoot" in block_data:
        receipt_root = Hash32(bytes(block_data["receiptsRoot"]))  # type: ignore
    elif "receiptRoot" in block_data:
        receipt_root = Hash32(bytes(block_data["receiptRoot"]))
    elif "receipts_root" in block_data:
        receipt_root = Hash32(
            to_bytes(hexstr=block_data["receipts_root"])  # type: ignore
        )
    else:
        raise Exception(f"Cannot find receipts_root key: {block_data!r}")

    logs_bloom: bytes

    if "logsBloom" in block_data:
        logs_bloom = bytes(block_data["logsBloom"])
    elif "logs_bloom" in block_data:
        logs_bloom = bytes(block_data["logs_bloom"])  # type: ignore
    else:
        raise Exception(f"Cannot find logs_bloom key: {block_data!r}")

    return Header(
        hash=Hash32(bytes(block_data["hash"])),
        difficulty=to_bytes(block_data["difficulty"]),
        block_number=block_data["number"],
        gas_limit=block_data["gasLimit"],
        timestamp=block_data["timestamp"],
        coinbase=to_canonical_address(block_data["miner"]),
        parent_hash=Hash32(bytes(block_data["parentHash"])),
        uncles_hash=Hash32(bytes(block_data["sha3Uncles"])),
        state_root=Hash32(bytes(block_data["stateRoot"])),
        transaction_root=Hash32(bytes(block_data["transactionsRoot"])),
        receipt_root=receipt_root,
        bloom=logs_bloom,
        gas_used=block_data["gasUsed"],
        extra_data=bytes(block_data["extraData"]),
        # TODO: mix-hash isn't part of the block data?
        # mix_hash=Hash32(block_data['mixHash']),
        nonce=bytes(block_data["nonce"]),
        is_canonical=True,
    )


def extract_transaction(transaction_data: TxData) -> Transaction:
    if transaction_data["to"] is None:
        to_address = None
    else:
        to_address = to_canonical_address(transaction_data["to"])

    return Transaction(
        hash=Hash32(bytes(transaction_data["hash"])),
        nonce=transaction_data["nonce"],
        gas_price=transaction_data["gasPrice"],
        gas=transaction_data["gas"],
        to=to_address,
        value=to_bytes(transaction_data["value"]),
        data=to_bytes(hexstr=transaction_data["input"]),
        v=to_bytes(transaction_data["v"]),
        r=bytes(transaction_data["r"]),
        s=bytes(transaction_data["s"]),
        sender=to_canonical_address(transaction_data["from"]),
    )


def extract_receipt(receipt_data: TxReceipt) -> Receipt:
    state_root: Hash32
    try:
        state_root = Hash32(to_bytes(hexstr=receipt_data["root"]))
    except KeyError:
        state_root = Hash32(receipt_data['status'].to_bytes(32, 'big'))

    return Receipt(
        state_root=state_root,
        gas_used=receipt_data["gasUsed"],
        bloom=bytes(receipt_data["logsBloom"]),
        logs=extract_logs(receipt_data["logs"]),
    )


@to_tuple
def extract_logs(logs_data: Sequence[LogReceipt]) -> Iterator[Log]:
    for log_data in logs_data:
        yield extract_log(log_data)


def extract_log(log_data: LogReceipt) -> Log:
    return Log(
        address=to_canonical_address(log_data["address"]),
        topics=tuple(Hash32(bytes(topic)) for topic in log_data["topics"]),
        data=to_bytes(hexstr=log_data["data"]),
    )
