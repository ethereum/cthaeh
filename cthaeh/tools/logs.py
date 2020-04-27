import itertools
from typing import Optional, Sequence

from eth_typing import Address, BlockNumber, Hash32
from eth_utils import is_same_address
from sqlalchemy.orm.exc import NoResultFound

from cthaeh.filter import FilterParams
from cthaeh.session import Session
from cthaeh.models import Header, Log
from cthaeh.loader import get_or_create_topics

from .factories import (
    BlockFactory,
    BlockTransactionFactory,
    HeaderFactory,
    LogFactory,
    LogTopicFactory,
)


def check_filter_results(params: FilterParams, results: Sequence[Log]) -> None:
    for log in results:
        check_log_matches_filter(params, log)


def check_log_matches_filter(params: FilterParams, log: Log) -> None:
    # Check that log belongs to a canonical header
    assert log.receipt.transaction.block is not None

    header = log.receipt.transaction.block.header

    # Check address matches
    if isinstance(params.address, tuple):
        assert any(
            is_same_address(log.address, address)
            for address in params.address
        )
    elif params.address is not None:
        assert is_same_address(log.address, params.address)

    # Check block number in range
    if isinstance(params.from_block, int):
        assert header.block_number >= params.from_block

    if isinstance(params.to_block, int):
        assert header.block_number <= params.to_block

    # Check topics
    zipped_topics = itertools.zip_longest(params.topics, log.topics, fillvalue=None)
    for expected_topic, actual_topic in zipped_topics:
        if expected_topic is None:
            assert actual_topic is not None
        elif actual_topic is None:
            assert expected_topic is None
        elif isinstance(expected_topic, tuple):
            assert any(topic == actual_topic.topic for topic in expected_topic)
        elif isinstance(expected_topic, bytes):
            assert expected_topic == actual_topic.topic
        else:
            raise Exception("Invariant")


def construct_log(session: Session,
                  *,
                  block_number: Optional[BlockNumber] = None,
                  address: Optional[Address] = None,
                  topics: Sequence[Hash32] = (),
                  data: bytes = b'',
                  is_canonical: bool = True,
                  ) -> Log:
    if block_number is not None:
        try:
            header = session.query(Header).filter(
                Header.is_canonical == is_canonical
            ).filter(
                Header.block_number == block_number
            ).one()
        except NoResultFound:
            header = HeaderFactory(
                is_canonical=is_canonical,
                block_number=block_number,
            )
    else:
        header = HeaderFactory(is_canonical=is_canonical)

    session.add(header)

    topic_objs = get_or_create_topics(session, topics)

    session.add_all(topic_objs)

    if is_canonical:
        log = LogFactory(
            receipt__transaction__block__header=header,
            address=address,
            data=data,
        )
        block_transaction = BlockTransactionFactory(
            idx=0,
            block=log.receipt.transaction.block,
            transaction=log.receipt.transaction,
        )
        session.add(block_transaction)
    else:
        log = LogFactory(
            receipt__transaction__block=None,
        )
        block = BlockFactory(header=header)
        block_transaction = BlockTransactionFactory(
            idx=0,
            block=block,
            transaction=log.receipt.transaction,
        )
        session.add_all((block, block_transaction))

    log_topics = tuple(
        LogTopicFactory(idx=idx, log=log, topic=topic)
        for idx, topic in enumerate(topic_objs)
    )

    session.add(log)
    session.add_all(log_topics)

    return log
