import pytest
from web3 import EthereumTesterProvider, Web3

from cthaeh.constants import GENESIS_PARENT_HASH, BLANK_ROOT_HASH
from cthaeh.exfiltration import retrieve_block


@pytest.fixture
def w3():
    return Web3(EthereumTesterProvider())


@pytest.mark.trio
async def test_exfiltration_of_genesis_block(w3):
    block = await retrieve_block(w3, 0)

    assert len(block.transactions) == 0
    assert len(block.uncles) == 0
    assert len(block.receipts) == 0

    header = block.header
    assert header.block_number == 0
    assert header.parent_hash == GENESIS_PARENT_HASH
    assert header.bloom == b''
    assert header.transaction_root == BLANK_ROOT_HASH
    assert header.receipt_root == BLANK_ROOT_HASH
