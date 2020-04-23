import logging
import os

from async_service import background_trio_service, Service

from cthaeh.cli_parser import parser
from cthaeh.logging import setup_logging


ALEXANDRIA_HEADER = "\n".join((
    ""
    r"       _-_",
    r"    /~~   ~~\\",
    r" /~~         ~~\\",  # noqa: E225
    r"{               }",
    r" \  _-     -_  /",
    r"   ~  \\ //  ~",
    r"_- -   | | _- _",
    r"  _ -  | |   -_",
    r"      // \\",
    ""
))


logger = logging.getLogger('cthaeh')


class Application(Service):
    logger = logging.getLogger('cthaeh.Cthaeh')

    def __init__(self) -> None:
        ...

    async def run(self) -> None:
        ...


async def main() -> None:
    app = Application()

    args = parser.parse_args()
    setup_logging(args.log_level)

    logger.info(ALEXANDRIA_HEADER)
    logger.info("Started main process (pid=%d)", os.getpid())
    async with background_trio_service(app) as manager:
        await manager.wait_finished()
