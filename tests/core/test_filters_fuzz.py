import itertools
import random
from typing import Callable, Dict, Iterator, TypeVar

from eth_typing import Address, Hash32
from eth_utils import int_to_big_endian, to_tuple
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy import orm

from hypothesis import (
    settings,
    strategies as st,
    given,
)

from cthaeh.filter import FilterParams, filter_logs
from cthaeh.models import Log, Header, Receipt, Topic
from cthaeh.tools.logs import check_filter_results
from cthaeh.tools.factories import (
    AddressFactory,
    BlockFactory,
    BlockTransactionFactory,
    Hash32Factory,
    HeaderFactory,
    LogFactory,
    LogTopicFactory,
    ReceiptFactory,
    TopicFactory,
    TransactionFactory,
)


T = TypeVar('T')


class ThingGenerator(Callable[[], T]):
    _population: Dict[int, T]

    def __init__(self, factory: Callable[[], T]):
        self._factory = factory
        self._population = {}

    def __call__(self) -> T:
        key = int(random.expovariate(2 / max(1, len(self._population))))
        if key not in self._population:
            self._population[key] = self._factory()
        return self._population[key]


@to_tuple
def build_log_topics(session: orm.Session,
                     topic_factory: ThingGenerator[Hash32]) -> Iterator[Log]:
    num_topics = int(random.expovariate(0.1))
    for idx in range(num_topics):
        topic = topic_factory()

        try:
            yield session.query(Topic).filter(Topic.topic == topic).one()
        except NoResultFound:
            yield TopicFactory(topic=topic)


def build_log(session,
              idx: int,
              receipt: Receipt,
              topic_factory: ThingGenerator[Hash32],
              address_factory: ThingGenerator[Address],
              ) -> Iterator[Log]:
    topics = build_log_topics(session, topic_factory)
    address = address_factory()

    data_size = int(random.expovariate(0.01))
    if data_size == 0:
        data = b''
    else:
        data = int_to_big_endian(random.getrandbits(data_size))
    log = LogFactory(idx=idx, receipt=receipt, address=address, data=data)

    logtopics = tuple(
        LogTopicFactory(idx=idx, log=log, topic_topic=topic)
        for idx, topic in enumerate(topics)
    )
    return log, logtopics


@to_tuple
def build_block_chain(session: orm.Session,
                      topic_factory: ThingGenerator[Hash32],
                      address_factory: ThingGenerator[Address],
                      num_blocks: int,
                      ) -> Iterator[Header]:
    for block_number in range(num_blocks):
        if block_number == 0:
            parent_hash = None
        else:
            parent = session.query(Header).filter(
                Header.block_number == block_number - 1,
                Header.is_canonical.is_(True),
            ).one()
            parent_hash = parent.hash

        header = HeaderFactory(block_number=block_number, _parent_hash=parent_hash)
        block = BlockFactory(header=header)

        num_transactions = int(random.expovariate(0.1))

        transactions = tuple(
            TransactionFactory(block=block)
            for _ in range(num_transactions)
        )
        blocktransactions = tuple(
            BlockTransactionFactory(
                idx=idx,
                block=block,
                transaction=transaction,
            ) for idx, transaction in enumerate(transactions)
        )
        receipts = tuple(
            ReceiptFactory(transaction=transaction)
            for transaction in transactions
        )
        num_logs_per_transaction = tuple(
            int(random.expovariate(0.1))
            for transaction in transactions
        )
        log_bundles = tuple(
            build_log(session, idx, receipt, topic_factory, address_factory)
            for num_logs, receipt in zip(num_logs_per_transaction, receipts)
            for idx in range(num_logs)
        )
        if log_bundles:
            logs, logtopic_bundles = zip(*log_bundles)
            logtopics = tuple(itertools.chain(*logtopic_bundles))
        else:
            logs, logtopics = (), ()

        session.add(header)
        session.add(block)
        session.add_all(transactions)
        session.add_all(blocktransactions)
        session.add_all(receipts)
        session.add_all(logs)
        session.add_all(logtopics)

        yield header


def build_filter(topic_factory: ThingGenerator[Address],
                 address_factory: ThingGenerator[Address]) -> FilterParams:
    num_topics = int(random.expovariate(0.1))
    topics = tuple(topic_factory() for _ in range(num_topics))
    address = address_factory()

    from_block, to_block = sorted((
        int(random.expovariate(0.05)),
        int(random.expovariate(0.01)),
    ))
    if random.randint(0, 20) == 0:
        from_block = None
    if random.randint(0, 2) == 0:
        to_block = None

    return FilterParams(
        from_block=from_block,
        to_block=to_block,
        address=address,
        topics=topics,
    )


MAX_BLOCK_COUNT = 5


@settings(deadline=20000, max_examples=5)
@given(
    num_blocks=st.integers(min_value=0, max_value=MAX_BLOCK_COUNT),
    random_module=st.random_module(),
)
def test_filters_fuzzy(_Session,
                       num_blocks,
                       random_module,
                       ):
    topic_factory = ThingGenerator(Hash32Factory)
    address_factory = ThingGenerator(AddressFactory)

    session = _Session()
    transaction = session.begin_nested()

    try:
        build_block_chain(session, topic_factory, address_factory, num_blocks)
        for _ in range(200):
            params = build_filter(topic_factory, address_factory)

            results = filter_logs(session, params)

            check_filter_results(params, results)
    finally:
        transaction.rollback()
        session.close()
