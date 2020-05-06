import logging
import pathlib
from typing import Optional

from async_service import Service
from eth_typing import BlockNumber
from sqlalchemy import orm
from sqlalchemy.orm import aliased
import trio
from web3 import Web3

from cthaeh.exfiltration import HeadExfiltrator, retrieve_block_number
from cthaeh.ir import Block as BlockIR
from cthaeh.loader import HeadLoader, HistoryFiller
from cthaeh.models import Header
from cthaeh.rpc import RPCServer


def determine_start_block(session: orm.Session) -> BlockNumber:
    child_header = aliased(Header)
    history_head = (
        session.query(Header)  # type: ignore
        .outerjoin(child_header, Header.hash == child_header._parent_hash)
        .filter(
            Header.is_canonical.is_(True),  # type: ignore
            child_header._parent_hash.is_(None),
        )
        .order_by(Header.block_number)
        .first()
    )

    if history_head is None:
        return BlockNumber(0)
    else:
        return BlockNumber(history_head.block_number + 1)


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

        self._session = session
        self.w3 = w3
        self.start_block = start_block
        self.end_block = end_block
        self.concurrency = concurrency

        if ipc_path is not None:
            self.rpc_server = RPCServer(ipc_path=ipc_path, session=session)

    async def run(self) -> None:
        head_height = await retrieve_block_number(self.w3)

        if self.end_block is None or self.end_block < head_height:
            head_send_channel, head_receive_channel = trio.open_memory_channel[BlockIR](
                128
            )

            head_exfiltrator = HeadExfiltrator(
                w3=self.w3,
                start_at=head_height,
                end_at=self.end_block,
                block_send_channel=head_send_channel,  # type: ignore
                concurrency_factor=self.concurrency,
            )
            head_loader = HeadLoader(
                session=self._session,
                block_receive_channel=head_receive_channel,  # type: ignore
            )
            self.manager.run_daemon_child_service(head_exfiltrator)
            self.manager.run_daemon_child_service(head_loader)

        history_filler = HistoryFiller(
            session=self._session,
            w3=self.w3,
            concurrency=self.concurrency,
            start_height=self.start_block,
            end_height=head_height if self.end_block is None else self.end_block,
        )
        history_filler_manager = self.manager.run_child_service(history_filler)

        if self.rpc_server is not None:
            self.manager.run_daemon_child_service(self.rpc_server)

        await history_filler_manager.wait_finished()
