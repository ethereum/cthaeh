import logging
import pathlib
from typing import Optional

from async_service import Service
from eth_typing import BlockNumber
from sqlalchemy import orm
import trio
from web3 import Web3

from cthaeh.exfiltration import HistoryExfiltrator, HeadExfiltrator, retrieve_block_number
from cthaeh.ir import Block as BlockIR
from cthaeh.loader import HeadLoader, HistoryLoader
from cthaeh.models import Header
from cthaeh.rpc import RPCServer


def determine_start_block(session: orm.Session) -> BlockNumber:
    head = (
        session.query(Header)  # type: ignore
        .order_by(Header.block_number.desc())
        .filter(Header.is_canonical == True)  # noqa: E712
        .first()
    )
    if head is None:
        return BlockNumber(0)
    else:
        return BlockNumber(head.block_number + 1)


class Application(Service):
    logger = logging.getLogger("cthaeh.Cthaeh")
    rpc_server: Optional[RPCServer] = None

    def __init__(
        self,
        w3: Web3,
        session: orm.Session,
        start_block: Optional[BlockNumber],
        end_block: Optional[BlockNumber],
        concurrency: int,
        ipc_path: Optional[pathlib.Path],
    ) -> None:
        if start_block is None:
            start_block = determine_start_block(session)

        self.w3 = w3
        self.start_block = start_block
        self.end_block = end_block
        self.concurrency = concurrency

        self._history_send_channel, history_receive_channel = trio.open_memory_channel[BlockIR](
            128
        )
        self._head_send_channel, head_receive_channel = trio.open_memory_channel[BlockIR](
            128
        )

        self.history_loader = HistoryLoader(
            session=session, block_receive_channel=history_receive_channel
        )
        self.head_loader = HeadLoader(
            session=session, block_receive_channel=head_receive_channel
        )

        if ipc_path is not None:
            self.rpc_server = RPCServer(ipc_path=ipc_path, session=session)

    async def run(self) -> None:
        head_height = await retrieve_block_number(self.w3)

        if self.end_block is None or self.end_block < head_height:
            head_exfiltrator = HeadExfiltrator(
                w3=self.w3,
                start_at=head_height,
                end_at=self.end_block,
                block_send_channel=self._head_send_channel.clone(),
                concurrency_factor=self.concurrency,
            )
            self.manager.run_daemon_child_service(head_exfiltrator)
            self.manager.run_daemon_child_service(self.head_loader)

            history_exfiltrator = HistoryExfiltrator(
                w3=self.w3,
                block_send_channel=self._history_send_channel,
                start_at=self.start_block,
                end_at=head_height,
                concurrency_factor=self.concurrency,
            )
            self.manager.run_child_service(history_exfiltrator)
            self.manager.run_child_service(self.history_loader)
        else:
            history_exfiltrator = HistoryExfiltrator(
                w3=self.w3,
                block_send_channel=self._history_send_channel,
                start_at=self.start_block,
                end_at=self.end_block,
                concurrency_factor=self.concurrency,
            )
            self.manager.run_child_service(history_exfiltrator)
            self.manager.run_child_service(self.history_loader)

        if self.rpc_server is not None:
            self.manager.run_daemon_child_service(self.rpc_server)

        await self.manager.wait_finished()
