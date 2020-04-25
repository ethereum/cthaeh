import logging

from cthaeh.cli_parser import parser
from cthaeh.logging import setup_logging


ALEXANDRIA_HEADER = "\n".join((
    "",
    r"       _-_",
    r"    /~~   ~~\\",
    r" /~~         ~~\\",  # noqa: E225
    r"{    CTHAEH     }",
    r" \  _-     -_  /",
    r"   ~  \\ //  ~",
    r"_- -   | | _- _",
    r"  _ -  | |   -_",
    r"      // \\",
    "",
))


logger = logging.getLogger('cthaeh')


async def main() -> None:
    args = parser.parse_args()

    setup_logging(args.log_level)

    logger.info(ALEXANDRIA_HEADER)

    await args.func(args)
