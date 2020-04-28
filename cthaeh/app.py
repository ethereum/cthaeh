import logging
import pathlib
from typing import Optional

from async_service import Service
from eth_typing import BlockNumber
from sqlalchemy import orm
import trio
from web3 import Web3

from cthaeh.exfiltration import Exfiltrator
from cthaeh.ir import Block as BlockIR
from cthaeh.loader import BlockLoader
from cthaeh.models import Header
from cthaeh.rpc import RPCServer


def determine_start_block(session: orm.Session) -> BlockNumber:
    head = session.query(Header).order_by(  # type: ignore
        Header.block_number.desc()
    ).filter(
        Header.is_canonical == True  # noqa: E712
    ).first()
    if head is None:
        return BlockNumber(0)
    else:
        return BlockNumber(head.block_number + 1)


class Application(Service):
    logger = logging.getLogger('cthaeh.Cthaeh')
    rpc_server: Optional[RPCServer] = None

    def __init__(self,
                 w3: Web3,
                 session: orm.Session,
                 start_block: Optional[BlockNumber],
                 end_block: Optional[BlockNumber],
                 concurrency: int,
                 ipc_path: Optional[pathlib.Path],
                 ) -> None:
        block_send_channel, block_receive_channel = trio.open_memory_channel[BlockIR](128)
        if start_block is None:
            start_block = determine_start_block(session)

        self.exfiltrator = Exfiltrator(
            w3=w3,
            block_send_channel=block_send_channel,
            start_at=start_block,
            end_at=end_block,
            concurrency_factor=concurrency,
        )
        self.loader = BlockLoader(
            session=session,
            block_receive_channel=block_receive_channel,
        )
        if ipc_path is not None:
            self.rpc_server = RPCServer(
                ipc_path=ipc_path,
                session=session,
            )

    async def run(self) -> None:
        self.manager.run_daemon_child_service(self.exfiltrator)
        self.manager.run_daemon_child_service(self.loader)
        if self.rpc_server is not None:
            self.manager.run_daemon_child_service(self.rpc_server)
        await self.manager.wait_finished()
