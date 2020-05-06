import secrets

from cthaeh.ir import Block, Header, Log, Receipt, Transaction

from .factories_common import AddressFactory, Hash32Factory

try:
    import factory
except ImportError as err:
    raise ImportError(
        "The `factory-boy` library is required to use the `alexandria.tools.factories` module"
    ) from err


class HeaderFactory(factory.Factory):  # type: ignore
    class Meta:
        model = Header

    hash = factory.LazyFunction(Hash32Factory)

    is_canonical = True

    parent_hash = factory.LazyFunction(Hash32Factory)

    uncles_hash = factory.LazyFunction(Hash32Factory)
    coinbase = factory.LazyFunction(AddressFactory)

    state_root = factory.LazyFunction(Hash32Factory)
    transaction_root = factory.LazyFunction(Hash32Factory)
    receipt_root = factory.LazyFunction(Hash32Factory)

    bloom = b""

    difficulty = b"\x01"
    block_number = 0
    gas_limit = 3141592
    gas_used = 3141592
    timestamp = 0
    extra_data = b""
    # mix_hash = factory.LazyFunction(Hash32Factory)
    nonce = factory.LazyFunction(lambda: secrets.token_bytes(8))


class BlockFactory(factory.Factory):  # type: ignore
    class Meta:
        model = Block

    header = factory.SubFactory(HeaderFactory)
    transactions = ()
    uncles = ()
    receipts = ()


class TransactionFactory(factory.Factory):  # type: ignore
    class Meta:
        model = Transaction

    # TODO: Compute via RLP
    hash = factory.LazyFunction(Hash32Factory)

    nonce = 0
    gas_price = 1
    gas = 21000
    to = factory.LazyFunction(AddressFactory)
    value = b"\x00"
    data = b""
    v = b"\x00" * 32
    r = b"\x00" * 32
    s = b"\x00" * 32

    sender = factory.LazyFunction(AddressFactory)


class ReceiptFactory(factory.Factory):  # type: ignore
    class Meta:
        model = Receipt

    state_root = factory.LazyFunction(Hash32Factory)
    bloom = b""
    gas_used = 21000


class LogFactory(factory.Factory):  # type: ignore
    class Meta:
        model = Log

    address = factory.LazyFunction(AddressFactory)
    data = b""
