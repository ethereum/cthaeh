import pathlib
import tempfile

from async_service import background_trio_service
from eth_utils import encode_hex, is_same_address, to_dict
import pytest
import trio
from web3 import IPCProvider, Web3

from cthaeh.filter import FilterParams, filter_logs
from cthaeh.rpc import RPCServer
from cthaeh.tools.logs import construct_log


@pytest.fixture
def ipc_path():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield pathlib.Path(temp_dir) / "jsonrpc.ipc"


@pytest.fixture
async def rpc_node(ipc_path, session):
    rpc_server = RPCServer(ipc_path, session)
    async with background_trio_service(rpc_server):
        await rpc_server.wait_serving()
        yield rpc_server


@pytest.fixture
async def w3(ipc_path, rpc_node):
    provider = IPCProvider(str(ipc_path))
    w3 = Web3(provider=provider)
    return w3


LOG_ADDRESS = b":primary".zfill(20)
OTHER_ADDRESS = b":other".zfill(20)

LOG_TOPIC_0 = b":topic-0".zfill(32)
LOG_TOPIC_1 = b":topic-1".zfill(32)
OTHER_TOPIC = b":other-topic".zfill(32)


def _rpc_friendly_topic(topic):
    if topic is None:
        return None
    elif isinstance(topic, bytes):
        return encode_hex(topic)
    elif isinstance(topic, tuple):
        return tuple(encode_hex(sub_topic) for sub_topic in topic)
    else:
        raise TypeError(f"Unsupported topic: {topic!r}")


@to_dict
def _params_to_rpc_request(params):
    if params.address is not None:
        yield "address", params.address
    if params.from_block is not None:
        yield "fromBlock", params.from_block
    if params.to_block is not None:
        yield "toBlock", params.to_block
    yield "topics", [_rpc_friendly_topic(topic) for topic in params.topics]


@pytest.mark.parametrize(
    "params",
    (
        FilterParams(None, None, None, ()),
        FilterParams(0, None, None, ()),
        FilterParams(None, 4, None, ()),
        FilterParams(0, 4, None, ()),
        FilterParams(0, 4, None, ()),
        FilterParams(None, None, LOG_ADDRESS, ()),
        FilterParams(0, 4, LOG_ADDRESS, ()),
        FilterParams(None, None, (LOG_ADDRESS, OTHER_ADDRESS), ()),
        FilterParams(None, None, None, (LOG_TOPIC_0,)),
        FilterParams(None, None, None, (LOG_TOPIC_0, LOG_TOPIC_1)),
        FilterParams(None, None, None, (None, LOG_TOPIC_1)),
        FilterParams(None, None, None, ((LOG_TOPIC_0, OTHER_TOPIC), LOG_TOPIC_1)),
        FilterParams(None, None, None, (None, (OTHER_TOPIC, LOG_TOPIC_1))),
    ),
)
@pytest.mark.trio
async def test_rpc_getLogs(session, w3, rpc_node, params):
    log = construct_log(
        session, block_number=2, address=LOG_ADDRESS, topics=(LOG_TOPIC_0, LOG_TOPIC_1)
    )
    expected_results = filter_logs(session, params)
    assert len(expected_results) == 1

    results = await trio.to_thread.run_sync(
        w3.eth.getLogs, _params_to_rpc_request(params)
    )
    assert len(results) == 1

    result = results[0]
    assert is_same_address(log.address, result["address"])
