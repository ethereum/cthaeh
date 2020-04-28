import argparse
import logging
import os
import sys

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from async_service import background_trio_service

from cthaeh.app import Application
from cthaeh.models import Base
from cthaeh.session import Session
from cthaeh.xdg import get_xdg_cthaeh_root


logger = logging.getLogger('cthaeh')


def _get_engine(args: argparse.Namespace) -> Engine:
    xdg_root = get_xdg_cthaeh_root()
    if not xdg_root.exists():
        xdg_root.mkdir(parents=True, exist_ok=True)

    if args.database_url is not None:
        database_url = args.database_url
    else:
        db_path = xdg_root / 'db.sqlite3'
        database_url = f'sqlite:///{db_path.resolve()}'

    logger.info("Using database: %s", database_url)

    return create_engine(database_url)


async def do_initialize_database(args: argparse.Namespace) -> None:
    # Establish database connections
    engine = _get_engine(args)

    Base.metadata.create_all(engine)
    sys.exit(0)


MEMORY_DB = 'sqlite:///:memory:'


async def do_main(args: argparse.Namespace) -> None:
    # Establish database connections
    engine = _get_engine(args)
    Session.configure(bind=engine)
    session = Session()

    # Ensure database schema is present
    if args.database_url == MEMORY_DB:
        Base.metadata.create_all(engine)

    start_block = args.start_block
    end_block = args.end_block

    from web3.auto.ipc import w3

    if args.disable_jsonrpc:
        ipc_path = None
    elif args.ipc_path:
        ipc_path = args.ipc_path
    else:
        ipc_path = get_xdg_cthaeh_root() / 'jsonrpc.ipc'

    app = Application(
        w3,
        session,
        start_block=start_block,
        end_block=end_block,
        concurrency=args.concurrency,
        ipc_path=ipc_path,
    )

    logger.info("Started main process (pid=%d)", os.getpid())
    async with background_trio_service(app) as manager:
        await manager.wait_finished()
