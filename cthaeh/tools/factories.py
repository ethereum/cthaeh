import secrets

try:
    import factory
except ImportError as err:
    raise ImportError(
        'The `factory-boy` library is required to use the `alexandria.tools.factories` module'
    ) from err


from cthaeh.constants import GENESIS_PARENT_HASH
from cthaeh.models import (
    Block,
    BlockTransaction,
    BlockUncle,
    Header,
    Transaction,
    Receipt,
    Log,
    LogTopic,
    Topic,
)

from cthaeh.session import Session


def AddressFactory():
    return secrets.token_bytes(20)


def Hash32Factory():
    return secrets.token_bytes(32)


class HeaderFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = Header
        sqlalchemy_session = Session

    hash = factory.LazyFunction(Hash32Factory)

    is_canonical = True

    _parent_hash = GENESIS_PARENT_HASH

    uncles_hash = factory.LazyFunction(Hash32Factory)
    coinbase = factory.LazyFunction(AddressFactory)

    state_root = factory.LazyFunction(Hash32Factory)
    transaction_root = factory.LazyFunction(Hash32Factory)
    receipt_root = factory.LazyFunction(Hash32Factory)

    _bloom = b''

    difficulty = b'\x01'
    block_number = 0
    gas_limit = 3141592
    gas_used = 3141592
    timestamp = 0
    extra_data = b''
    # mix_hash = factory.LazyFunction(Hash32Factory)
    nonce = factory.LazyFunction(lambda: secrets.token_bytes(8))


class BlockFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = Block
        sqlalchemy_session = Session

    header = factory.SubFactory(HeaderFactory)


class BlockUncleFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = BlockUncle
        sqlalchemy_session = Session

    block = factory.SubFactory(BlockFactory)
    uncle = factory.SubFactory(HeaderFactory)


class TransactionFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = Transaction
        sqlalchemy_session = Session

    # TODO: Compute via RLP
    hash = factory.LazyFunction(Hash32Factory)

    block = factory.SubFactory(BlockFactory)

    nonce = 0
    gas_price = 1
    gas = 21000
    to = factory.LazyFunction(AddressFactory)
    value = b'\x00'
    data = b''
    v = b'\x00' * 32
    r = b'\x00' * 32
    s = b'\x00' * 32

    sender = factory.LazyFunction(AddressFactory)


class BlockTransactionFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = BlockTransaction
        sqlalchemy_session = Session

    block = factory.SubFactory(BlockFactory)
    transaction = factory.SubFactory(TransactionFactory)


class ReceiptFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = Receipt
        sqlalchemy_session = Session

    transaction = factory.SubFactory(TransactionFactory)

    state_root = factory.LazyFunction(Hash32Factory)
    _bloom = b''
    gas_used = 21000


class LogFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = Log
        sqlalchemy_session = Session

    idx = 0
    receipt = factory.SubFactory(ReceiptFactory)

    address = factory.LazyFunction(AddressFactory)
    data = b''


class TopicFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = Topic
        sqlalchemy_session = Session

    topic = factory.LazyFunction(Hash32Factory)


class LogTopicFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = LogTopic
        sqlalchemy_session = Session

    topic = factory.SubFactory(TopicFactory)
    log = factory.SubFactory(LogFactory)
