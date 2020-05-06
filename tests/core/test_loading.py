from async_service import background_trio_service
from eth_utils import to_tuple
import pytest
import trio

from cthaeh.constants import GENESIS_PARENT_HASH
from cthaeh.exfiltration import HeadBlockPacket
from cthaeh.loader import HeadLoader, HistoryLoader
from cthaeh.models import Header
from cthaeh.tools.factories_common import Hash32Factory
from cthaeh.tools.factories_ir import (
    BlockFactory,
    HeaderFactory,
    LogFactory,
    ReceiptFactory,
    TransactionFactory,
)


def mk_transaction_bundle(topic_bundle):
    logs = tuple(LogFactory(topics=topics) for topics in topic_bundle)
    receipt = ReceiptFactory(logs=logs)
    transaction = TransactionFactory()
    return transaction, receipt


def mk_block(parent=None, topic_bundles=(), num_uncles=0):
    if topic_bundles:
        transactions, receipts = zip(
            *(mk_transaction_bundle(topic_bundle) for topic_bundle in topic_bundles)
        )
        receipt = receipts[0]
        assert isinstance(receipt.logs, tuple)
        first_log = receipt.logs[0]
        assert isinstance(first_log.address, bytes)
    else:
        transactions = ()
        receipts = ()

    if parent is None:
        parent_hash = GENESIS_PARENT_HASH
        block_number = 0
    else:
        parent_hash = parent.hash
        block_number = parent.block_number + 1

    if num_uncles == 0:
        uncles = ()
    else:
        if block_number == 0:
            raise Exception("nope")
        uncles = tuple(
            HeaderFactory(is_canonical=False, block_number=block_number - 1)
            for _ in range(num_uncles)
        )

    return BlockFactory(
        header__parent_hash=parent_hash,
        header__block_number=block_number,
        header__is_canonical=True,
        transactions=transactions,
        uncles=uncles,
        receipts=receipts,
    )


@to_tuple
def mk_topic_bundle(*topic_counts):
    for count in topic_counts:
        yield tuple(Hash32Factory() for _ in range(count))


@pytest.mark.trio
async def test_history_loader_from_genesis(session):
    send_channel, receive_channel = trio.open_memory_channel(0)
    loader = HistoryLoader(session, receive_channel, is_detatched=False)

    genesis = mk_block()
    block_1 = mk_block(parent=genesis.header, topic_bundles=(mk_topic_bundle(1),))
    block_2 = mk_block(parent=block_1.header, topic_bundles=(mk_topic_bundle(2, 1),))
    block_3 = mk_block(parent=block_2.header, topic_bundles=(mk_topic_bundle(3, 4),))

    assert session.query(Header).scalar() is None

    async with background_trio_service(loader) as manager:
        async with send_channel:
            await send_channel.send(genesis)
            await send_channel.send(block_1)
            await send_channel.send(block_2)
            await send_channel.send(block_3)

        with trio.fail_after(2):
            await manager.wait_finished()

    head = session.query(Header).filter(Header.hash == block_3.header.hash).one()
    assert head.hash == block_3.header.hash


@pytest.mark.trio
async def test_head_loader_without_reorgs(session):
    send_channel, receive_channel = trio.open_memory_channel(0)
    loader = HeadLoader(session, receive_channel)

    header_9 = HeaderFactory(block_number=9)
    block_10 = mk_block(parent=header_9, topic_bundles=(mk_topic_bundle(1),))
    block_11 = mk_block(parent=block_10.header, topic_bundles=(mk_topic_bundle(2, 1),))
    block_12 = mk_block(parent=block_11.header, topic_bundles=(mk_topic_bundle(3, 4),))
    block_13 = mk_block(parent=block_12.header, topic_bundles=(mk_topic_bundle(1, 2),))

    packet_0 = HeadBlockPacket(blocks=(block_10,))
    packet_1 = HeadBlockPacket(blocks=(block_11, block_12))
    packet_2 = HeadBlockPacket(blocks=(block_13,))

    assert session.query(Header).scalar() is None

    async with background_trio_service(loader) as manager:
        async with send_channel:
            await send_channel.send(packet_0)
            await send_channel.send(packet_1)
            await send_channel.send(packet_2)

        with trio.fail_after(2):
            await manager.wait_finished()

    front = session.query(Header).filter(Header.hash == block_10.header.hash).one()
    assert front.hash == block_10.header.hash
    assert front.is_detatched

    head = session.query(Header).filter(Header.hash == block_13.header.hash).one()
    assert head.hash == block_13.header.hash
    assert not head.is_detatched


@pytest.mark.trio
async def test_head_loader_with_single_block_reorg(session):
    send_channel, receive_channel = trio.open_memory_channel(0)
    loader = HeadLoader(session, receive_channel)

    header_9 = HeaderFactory(block_number=9)
    block_10 = mk_block(parent=header_9, topic_bundles=(mk_topic_bundle(1),))
    block_11 = mk_block(parent=block_10.header, topic_bundles=(mk_topic_bundle(2, 1),))
    block_12 = mk_block(parent=block_11.header, topic_bundles=(mk_topic_bundle(3, 4),))
    block_13 = mk_block(parent=block_12.header, topic_bundles=(mk_topic_bundle(1, 2),))
    orphan_14 = mk_block(parent=block_13.header, topic_bundles=(mk_topic_bundle(1, 2),))
    block_14 = mk_block(parent=block_13.header, topic_bundles=(mk_topic_bundle(2, 1),))

    packet_0 = HeadBlockPacket(blocks=(block_10,))
    packet_1 = HeadBlockPacket(blocks=(block_11, block_12))
    packet_2 = HeadBlockPacket(blocks=(block_13,))
    packet_3 = HeadBlockPacket(blocks=(orphan_14,))
    packet_4 = HeadBlockPacket(blocks=(block_14,), orphans=(orphan_14.header,))

    assert session.query(Header).scalar() is None

    async with background_trio_service(loader) as manager:
        async with send_channel:
            await send_channel.send(packet_0)
            await send_channel.send(packet_1)
            await send_channel.send(packet_2)
            await send_channel.send(packet_3)
            # Check that packet 3 data is in place.
            await send_channel.send(packet_4)

        with trio.fail_after(2):
            await manager.wait_finished()

    orphan = session.query(Header).filter(Header.hash == orphan_14.header.hash).one()
    assert orphan.hash == orphan_14.header.hash
    assert orphan.is_canonical is False

    head = session.query(Header).filter(Header.hash == block_14.header.hash).one()
    assert head.hash == block_14.header.hash


@pytest.mark.trio
async def test_head_loader_with_multi_block_reorg(session):
    send_channel, receive_channel = trio.open_memory_channel(0)
    loader = HeadLoader(session, receive_channel)

    header_9 = HeaderFactory(block_number=9)
    block_10 = mk_block(parent=header_9, topic_bundles=(mk_topic_bundle(1),))
    block_11 = mk_block(parent=block_10.header, topic_bundles=(mk_topic_bundle(2, 1),))
    block_12 = mk_block(parent=block_11.header, topic_bundles=(mk_topic_bundle(3, 4),))
    block_13 = mk_block(parent=block_12.header, topic_bundles=(mk_topic_bundle(1, 2),))
    orphan_14 = mk_block(parent=block_13.header, topic_bundles=(mk_topic_bundle(1, 2),))
    orphan_15 = mk_block(
        parent=orphan_14.header, topic_bundles=(mk_topic_bundle(1, 2),)
    )
    block_14 = mk_block(parent=block_13.header, topic_bundles=(mk_topic_bundle(2, 2),))
    # TODO: include orphan_14.header in block_15.uncles
    block_15 = mk_block(parent=block_14.header, topic_bundles=(mk_topic_bundle(4, 4),))

    packet_0 = HeadBlockPacket(blocks=(block_10,))
    packet_1 = HeadBlockPacket(blocks=(block_11, block_12))
    packet_2 = HeadBlockPacket(blocks=(block_13,))
    packet_3 = HeadBlockPacket(blocks=(orphan_14,))
    packet_4 = HeadBlockPacket(blocks=(orphan_15,))
    packet_5 = HeadBlockPacket(
        blocks=(block_14, block_15), orphans=(orphan_14.header, orphan_15.header)
    )

    assert session.query(Header).scalar() is None

    async with background_trio_service(loader) as manager:
        async with send_channel:
            await send_channel.send(packet_0)
            await send_channel.send(packet_1)
            await send_channel.send(packet_2)
            await send_channel.send(packet_3)
            await send_channel.send(packet_4)
            # Check that orphan fork is in place
            await send_channel.send(packet_5)

        with trio.fail_after(2):
            await manager.wait_finished()

    orphan_a = session.query(Header).filter(Header.hash == orphan_14.header.hash).one()
    assert orphan_a.hash == orphan_14.header.hash
    assert orphan_a.is_canonical is False

    orphan_b = session.query(Header).filter(Header.hash == orphan_15.header.hash).one()
    assert orphan_b.hash == orphan_15.header.hash
    assert orphan_b.is_canonical is False

    head = session.query(Header).filter(Header.hash == block_14.header.hash).one()
    assert head.hash == block_14.header.hash
    assert head.is_canonical

    head = session.query(Header).filter(Header.hash == block_15.header.hash).one()
    assert head.hash == block_15.header.hash
    assert head.is_canonical
