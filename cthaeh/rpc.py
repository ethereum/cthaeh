import collections
import io
import json
import logging
import pathlib
from typing import Any, List, Mapping, Optional, Tuple, cast

from eth_utils import ValidationError
from mypy_extensions import TypedDict
import trio

from async_service import Service


NEW_LINE = "\n"


def strip_non_json_prefix(raw_request: str) -> Tuple[str, str]:
    if raw_request and raw_request[0] != '{':
        prefix, bracket, rest = raw_request.partition('{')
        return prefix.strip(), bracket + rest
    else:
        return '', raw_request


async def write_error(socket: trio.socket.SocketType, message: str) -> None:
    json_error = json.dumps({'error': message})
    await socket.send(json_error.encode('utf8'))


def validate_request(request: Mapping[Any, Any]) -> None:
    try:
        version = request['jsonrpc']
    except KeyError as err:
        raise ValidationError("Missing 'jsonrpc' key") from err
    else:
        if version != '2.0':
            raise ValidationError(f"Invalid version: {version}")

    if 'method' not in request:
        raise ValidationError("Missing 'method' key")
    if 'params' in request:
        if not isinstance(request['params'], list):
            raise ValidationError("Missing 'method' key")


class RPCRequest(TypedDict):
    jsonrpc: str
    method: str
    params: List[Any]
    id: Optional[int]


def generate_response(request: RPCRequest, result: Any, error: Optional[str]) -> str:
    response = {
        'id': request.get('id', -1),
        'jsonrpc': request.get('jsonrpc', "2.0"),
    }

    if result is None and error is None:
        raise ValueError("Must supply either result or error for JSON-RPC response")
    if result is not None and error is not None:
        raise ValueError("Must not supply both a result and an error for JSON-RPC response")
    elif result is not None:
        response['result'] = result
    elif error is not None:
        response['error'] = str(error)
    else:
        raise Exception("Unreachable code path")

    return json.dumps(response)


class RPCServer(Service):
    logger = logging.getLogger('cthaeh.rpc.RPCServer')

    def __init__(self,
                 ipc_path: pathlib.Path,
                 ) -> None:
        self.ipc_path = ipc_path
        self._serving = trio.Event()

    async def wait_serving(self) -> None:
        await self._serving.wait()

    async def run(self) -> None:
        self.manager.run_daemon_task(self.serve, self.ipc_path)
        try:
            await self.manager.wait_finished()
        finally:
            self.ipc_path.unlink()

    async def execute_rpc(self, request: RPCRequest) -> str:
        namespaced_method = request['method']
        params = request.get('params', [])

        self.logger.debug('RPCServer handling request: %s', namespaced_method)

        namespace, _, method = namespaced_method.partition('_')
        if namespace != 'eth':
            return generate_response(request, None, f"Invalid namespace: {namespaced_method}")

        if method == "getLogs":
            return await self._handle_getLogs(request, *params)
        elif method == "getFilterChanges":
            raise NotImplementedError()
        elif method == "getFilterLogs":
            raise NotImplementedError()
        elif method == "newFilter":
            raise NotImplementedError()
        elif method == "uninstallFilter":
            raise NotImplementedError()
        else:
            return generate_response(request, None, f"Unknown method: {namespaced_method}")

    async def serve(self, ipc_path: pathlib.Path) -> None:
        self.logger.info("Starting RPC server over IPC socket: %s", ipc_path)

        with trio.socket.socket(trio.socket.AF_UNIX, trio.socket.SOCK_STREAM) as sock:
            # TODO: unclear if the following stuff is necessary:
            # ###################################################
            # These options help fix an issue with the socket reporting itself
            # already being used since it accepts many client connection.
            # https://stackoverflow.com/questions/6380057/python-binding-socket-address-already-in-use
            # sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # ###################################################
            await sock.bind(str(ipc_path))
            # Allow up to 10 pending connections.
            sock.listen(10)

            self._serving.set()

            while self.manager.is_running:
                conn, addr = await sock.accept()
                self.logger.debug('Server accepted connection: %r', addr)
                self.manager.run_task(self._handle_connection, conn)

    async def _handle_connection(self, socket: trio.socket.SocketType) -> None:
        buffer = io.StringIO()
        decoder = json.JSONDecoder()

        with socket:
            while True:
                data = await socket.recv(1024)
                buffer.write(data.decode())

                bad_prefix, raw_request = strip_non_json_prefix(buffer.getvalue())
                if bad_prefix:
                    self.logger.info("Client started request with non json data: %r", bad_prefix)
                    await write_error(socket, f'Cannot parse json: {bad_prefix}')
                    continue

                try:
                    request, offset = decoder.raw_decode(raw_request)
                except json.JSONDecodeError:
                    # invalid json request, keep reading data until a valid json is formed
                    if raw_request:
                        self.logger.debug(
                            "Invalid JSON, waiting for rest of message: %r",
                            raw_request,
                        )
                    else:
                        await trio.sleep(0.01)
                    continue

                if not isinstance(request, collections.Mapping):
                    self.logger.debug("Invalid payload: %s", type(request))
                    return

                # TODO: more efficient algorithm can be used here by
                # manipulating the buffer such that we can seek back to the
                # correct position for *new* data to come in.
                buffer.seek(0)
                buffer.write(raw_request[offset:])
                buffer.truncate()

                if not request:
                    self.logger.debug("Client sent empty request")
                    await write_error(socket, 'Invalid Request: empty')
                    continue

                try:
                    validate_request(request)
                except ValidationError as err:
                    await write_error(socket, str(err))
                    continue

                try:
                    result = await self.execute_rpc(cast(RPCRequest, request))
                except Exception as e:
                    self.logger.exception("Unrecognized exception while executing RPC")
                    await write_error(socket, "unknown failure: " + str(e))
                else:
                    if not result.endswith(NEW_LINE):
                        result += NEW_LINE

                    try:
                        await socket.send(result.encode())
                    except BrokenPipeError:
                        break

    #
    # RPC Method Handlers
    #
    async def _handle_getLogs(self, request: RPCRequest) -> str:
        raise NotImplementedError
