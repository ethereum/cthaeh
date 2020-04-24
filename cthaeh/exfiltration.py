import itertools
import logging
from typing import Iterator, Optional, Sequence

from async_service import Service
from eth_typing import BlockNumber, Hash32
from eth_utils import (
    big_endian_to_int,
    to_bytes,
    to_canonical_address,
    to_tuple,
)
import trio
from web3 import Web3
from web3.types import BlockData, LogReceipt, TxData, TxReceipt, Uncle

from cthaeh._utils import gather
from cthaeh.ir import Block, Header, Transaction, Receipt, Log


logger = logging.getLogger('cthaeh.exfiltration')


def iter_block_numbers(start_at: BlockNumber,
                       end_at: Optional[BlockNumber]) -> Iterator[BlockNumber]:
    if end_at is None:
        return itertools.count(start_at)
    else:
        return iter(range(start_at, end_at))


class Exfiltrator(Service):
    logger = logger

    def __init__(self,
                 w3: Web3,
                 start_at: BlockNumber,
                 end_at: Optional[BlockNumber],
                 block_send_channel: trio.abc.SendChannel[Block],
                 concurrency_factor: int = 3,
                 ) -> None:
        self.w3 = w3
        self.start_at = start_at
        self.end_at = end_at
        self._concurrency_factor = concurrency_factor
        self._block_send_channel = block_send_channel

    async def run(self) -> None:
        semaphor = trio.Semaphore(self._concurrency_factor, max_value=self._concurrency_factor)

        async def _fetch(block_number: int) -> None:
            # TODO: handle web3 failures
            block = await retrieve_block(self.w3, block_number)
            semaphor.release()
            await self._block_send_channel.send(block)

        async with self._block_send_channel:
            async with trio.open_nursery() as nursery:
                while self.manager.is_running:
                    for block_number in iter_block_numbers(self.start_at, self.end_aat):
                        await semaphor.acquire()
                        nursery.start_soon(_fetch, block_number)


async def retrieve_block(w3: Web3, block_number: int) -> Block:
    logger.debug('Retrieving header #%d', block_number)

    block_data = await trio.to_thread.run_sync(w3.eth.getBlock, block_number, True)
    transactions_data = block_data['transactions']

    transaction_hashes = tuple(tx_data['hash'] for tx_data in transactions_data)
    receipts_data = await gather(*(
        (trio.to_thread.run_sync, w3.eth.getTransactionReceipt, transaction_hash)
        for transaction_hash
        in transaction_hashes
    ))

    uncles_data = await gather(*(
        (trio.to_thread.run_sync, w3.eth.getUncleByBlock, block_data['hash'], idx)
        for idx in range(len(block_data['uncles']))
    ))

    return extract_block(
        block_data,
        transactions_data=transactions_data,
        uncles_data=uncles_data,
        receipts_data=receipts_data,
    )


def extract_block(block_data: BlockData,
                  transactions_data: Sequence[TxData],
                  uncles_data: Sequence[Uncle],
                  receipts_data: Sequence[TxReceipt],
                  ) -> Block:
    header = extract_header(block_data)
    transactions = tuple(extract_transaction(tx_data) for tx_data in transactions_data)
    uncles = tuple(extract_uncle(uncle_data) for uncle_data in uncles_data)
    receipts = tuple(extract_receipt(receipt_data) for receipt_data in receipts_data)
    return Block(
        header=header,
        transactions=transactions,
        uncles=uncles,
        receipts=receipts,
    )


def extract_uncle(uncle_data: Uncle) -> Header:
    receipt_root: Hash32

    if 'receiptsRoot' in uncle_data:
        receipt_root = Hash32(bytes(uncle_data['receiptsRoot']))
    elif 'receiptRoot' in uncle_data:
        receipt_root = Hash32(bytes(uncle_data['receiptRoot']))
    elif 'receipts_root' in uncle_data:
        receipt_root = Hash32(bytes(uncle_data['receipts_root']))
    else:
        raise Exception(f"Cannot find receipts_root key: {uncle_data!r}")

    logs_bloom: int

    if 'logsBloom' in uncle_data:
        logs_bloom = big_endian_to_int(bytes(uncle_data['logsBloom']))
    elif 'logs_bloom' in uncle_data:
        logs_bloom = big_endian_to_int(bytes(uncle_data['logs_bloom']))
    else:
        raise Exception(f"Cannot find logs_bloom key: {uncle_data!r}")

    return Header(
        hash=Hash32(bytes(uncle_data['hash'])),
        difficulty=uncle_data['difficulty'],
        block_number=uncle_data['number'],
        gas_limit=uncle_data['gasLimit'],
        timestamp=uncle_data['timestamp'],
        coinbase=to_canonical_address(uncle_data['author']),
        parent_hash=Hash32(bytes(uncle_data['parentHash'])),
        uncles_hash=Hash32(bytes(uncle_data['sha3Uncles'])),
        state_root=Hash32(bytes(uncle_data['stateRoot'])),
        transaction_root=Hash32(bytes(uncle_data['transactionsRoot'])),
        receipt_root=receipt_root,
        bloom=logs_bloom,
        gas_used=uncle_data['gasUsed'],
        extra_data=bytes(uncle_data['extraData']),
        # mix_hash=Hash32(uncle_data['mixHash']),
        nonce=bytes(uncle_data['nonce']),
        is_canonical=False,
    )


def extract_header(block_data: BlockData) -> Header:
    receipt_root: Hash32

    if 'receiptsRoot' in block_data:
        receipt_root = Hash32(bytes(block_data['receiptsRoot']))
    elif 'receiptRoot' in block_data:
        receipt_root = Hash32(bytes(block_data['receiptRoot']))
    elif 'receipts_root' in block_data:
        receipt_root = Hash32(to_bytes(hexstr=block_data['receipts_root']))
    else:
        raise Exception(f"Cannot find receipts_root key: {block_data!r}")

    logs_bloom: int

    if 'logsBloom' in block_data:
        logs_bloom = big_endian_to_int(bytes(block_data['logsBloom']))
    elif 'logs_bloom' in block_data:
        logs_bloom = big_endian_to_int(bytes(block_data['logs_bloom']))
    else:
        raise Exception(f"Cannot find logs_bloom key: {block_data!r}")

    return Header(
        hash=Hash32(bytes(block_data['hash'])),
        difficulty=block_data['difficulty'],
        block_number=block_data['number'],
        gas_limit=block_data['gasLimit'],
        timestamp=block_data['timestamp'],
        coinbase=to_canonical_address(block_data['miner']),
        parent_hash=Hash32(bytes(block_data['parentHash'])),
        uncles_hash=Hash32(bytes(block_data['sha3Uncles'])),
        state_root=Hash32(bytes(block_data['stateRoot'])),
        transaction_root=Hash32(bytes(block_data['transactionsRoot'])),
        receipt_root=receipt_root,
        bloom=logs_bloom,
        gas_used=block_data['gasUsed'],
        extra_data=bytes(block_data['extraData']),
        # TODO: mix-hash isn't part of the block data?
        # mix_hash=Hash32(block_data['mixHash']),
        nonce=bytes(block_data['nonce']),
        is_canonical=True,
    )


def extract_transaction(transaction_data: TxData) -> Transaction:
    return Transaction(
        nonce=transaction_data['nonce'],
        gas_price=transaction_data['gasPrice'],
        gas=transaction_data['gas'],
        to=to_canonical_address(transaction_data['to']),
        value=transaction_data['value'],
        data=bytes(transaction_data['data']),
        v=transaction_data['v'],
        r=big_endian_to_int(transaction_data['r']),
        s=big_endian_to_int(transaction_data['s']),
        sender=to_canonical_address(transaction_data['to']),
    )


def extract_receipt(receipt_data: TxReceipt) -> Receipt:
    return Receipt(
        state_root=Hash32(bytes(receipt_data['stateRoot'])),
        gas_used=receipt_data['gasUsed'],
        bloom=big_endian_to_int(receipt_data['logsBloom']),
        logs=extract_logs(receipt_data['logs']),
    )


@to_tuple
def extract_logs(logs_data: Sequence[LogReceipt]) -> Iterator[Log]:
    for log_data in logs_data:
        yield extract_log(log_data)


def extract_log(log_data: LogReceipt) -> Log:
    return Log(
        address=to_canonical_address(log_data['address']),
        topics=tuple(bytes(topic) for topic in log_data['topics']),
        data=bytes(log_data['data']),
    )
