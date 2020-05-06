import secrets

from eth_typing import Address, Hash32


def AddressFactory() -> Address:
    return Address(secrets.token_bytes(20))


def Hash32Factory() -> Hash32:
    return Hash32(secrets.token_bytes(32))
