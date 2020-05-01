from async_service import background_trio_service
import pytest
import trio
from web3 import Web3, EthereumTesterProvider

from cthaeh.app import Application
from cthaeh.exfiltration import retrieve_block
from cthaeh.loader import import_block
from cthaeh.models import Header


CHAIN_HEIGHT = 10


@pytest.fixture
def w3():
    _w3 = Web3(EthereumTesterProvider())
    _w3.testing.mine(CHAIN_HEIGHT)
    return _w3


async def _load_block(session, w3, block_number, is_detatched=False) -> None:
    block = await retrieve_block(w3, block_number)
    import_block(session, block, {}, is_detatched)


def _verify_integrity(session, from_block, to_block, is_detatched=False) -> None:
    for block_number in range(from_block, to_block):
        header = session.query(Header).filter(
            Header.is_canonical.is_(True),
            Header.block_number == block_number,
        ).one()

        if block_number == from_block:
            if header.is_genesis:
                continue
            assert header.is_detatched is is_detatched
        else:
            assert not header.is_detatched


def _verify_empty(session, from_block, to_block) -> None:
    range_constraints = []
    if from_block is not None:
        range_constraints.append(Header.block_number >= from_block)
    if to_block is not None:
        range_constraints.append(Header.block_number < to_block)

    assert session.query(Header).filter(
        Header.is_canonical.is_(True),
        *range_constraints
    ).count() == 0


@pytest.mark.trio
async def test_app_empty_database_null_start_fixed_end(session, w3):
    app = Application(
        w3=w3,
        session=session,
        start_block=None,
        end_block=CHAIN_HEIGHT,
        concurrency=1,
        ipc_path=None,
    )
    with trio.fail_after(5):
        async with background_trio_service(app) as manager:
            await manager.wait_finished()

    _verify_integrity(session, 0, CHAIN_HEIGHT)
    _verify_empty(session, CHAIN_HEIGHT, None)


@pytest.mark.trio
async def test_app_empty_database_fixed_start_fixed_end(session, w3):
    app = Application(
        w3=w3,
        session=session,
        start_block=5,
        end_block=CHAIN_HEIGHT,
        concurrency=1,
        ipc_path=None,
    )
    with trio.fail_after(5):
        async with background_trio_service(app) as manager:
            await manager.wait_finished()

    _verify_integrity(session, 5, CHAIN_HEIGHT, is_detatched=True)
    _verify_empty(session, None, 5)
    _verify_empty(session, CHAIN_HEIGHT, None)


@pytest.mark.trio
async def test_app_first_blocks_loaded_null_start_fixed_end(session, w3):
    await _load_block(session, w3, 0)
    await _load_block(session, w3, 1)
    await _load_block(session, w3, 2)

    app = Application(
        w3=w3,
        session=session,
        start_block=None,
        end_block=CHAIN_HEIGHT,
        concurrency=1,
        ipc_path=None,
    )
    with trio.fail_after(5):
        async with background_trio_service(app) as manager:
            await manager.wait_finished()

    _verify_integrity(session, 0, CHAIN_HEIGHT)
    _verify_empty(session, CHAIN_HEIGHT, None)


@pytest.mark.trio
async def test_app_two_ranges_loaded_null_start_fixed_end(session, w3):
    await _load_block(session, w3, 0)
    await _load_block(session, w3, 1)
    await _load_block(session, w3, 2)

    await _load_block(session, w3, 5, is_detatched=True)
    await _load_block(session, w3, 6)
    await _load_block(session, w3, 7)

    app = Application(
        w3=w3,
        session=session,
        start_block=None,
        end_block=CHAIN_HEIGHT,
        concurrency=1,
        ipc_path=None,
    )
    with trio.fail_after(5):
        async with background_trio_service(app) as manager:
            await manager.wait_finished()

    _verify_integrity(session, 0, CHAIN_HEIGHT)
    _verify_empty(session, CHAIN_HEIGHT, None)


@pytest.mark.trio
async def test_app_fully_loaded_null_start_fixed_end(session, w3):
    for block_number in range(CHAIN_HEIGHT):
        await _load_block(session, w3, block_number)

    app = Application(
        w3=w3,
        session=session,
        start_block=None,
        end_block=CHAIN_HEIGHT,
        concurrency=1,
        ipc_path=None,
    )
    with trio.fail_after(5):
        async with background_trio_service(app) as manager:
            await manager.wait_finished()

    _verify_integrity(session, 0, CHAIN_HEIGHT)
    _verify_empty(session, CHAIN_HEIGHT, None)


@pytest.mark.trio
async def test_app_empty_db_null_start_fixed_end_with_head_tracking(session, w3):
    app = Application(
        w3=w3,
        session=session,
        start_block=None,
        end_block=CHAIN_HEIGHT + 2,
        concurrency=1,
        ipc_path=None,
    )
    with trio.fail_after(10):
        async with background_trio_service(app) as manager:
            await trio.sleep(0.5)
            for _ in range(2):
                await trio.sleep(0.05)
                w3.testing.mine()
            await manager.wait_finished()

    _verify_integrity(session, 0, CHAIN_HEIGHT + 2)
    _verify_empty(session, CHAIN_HEIGHT + 2, None)


@pytest.mark.trio
async def test_app_null_start_fixed_end_with_head_tracking(session, w3):
    await _load_block(session, w3, 0)
    await _load_block(session, w3, 1)
    await _load_block(session, w3, 2)

    await _load_block(session, w3, 8, is_detatched=True)
    await _load_block(session, w3, 9)

    app = Application(
        w3=w3,
        session=session,
        start_block=None,
        end_block=CHAIN_HEIGHT + 2,
        concurrency=1,
        ipc_path=None,
    )
    with trio.fail_after(10):
        async with background_trio_service(app) as manager:
            await trio.sleep(0.5)
            for _ in range(2):
                await trio.sleep(0.05)
                w3.testing.mine()
            await manager.wait_finished()

    _verify_integrity(session, 0, CHAIN_HEIGHT + 2)
    _verify_empty(session, CHAIN_HEIGHT + 2, None)
