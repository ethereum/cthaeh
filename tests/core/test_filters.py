from cthaeh.filter import filter, FilterParams
from cthaeh.tools.logs import construct_log, check_log_matches_filter
from cthaeh.tools.factories import AddressFactory


def test_filter_log_empty_params(session):
    log = construct_log(session)

    params = FilterParams()

    results = filter(session, params)

    for result in results:
        check_log_matches_filter(params, result)

    assert len(results) == 1
    assert results[0].id == log.id


def test_filter_log_single_address_match(session):
    address = AddressFactory()
    log = construct_log(session, address=address)

    params = FilterParams(address=address)

    results = filter(session, params)

    for result in results:
        check_log_matches_filter(params, result)

    assert len(results) == 1
    assert results[0].id == log.id
    assert results[0].address == address


def test_filter_log_multiple_addresses(session):
    address = AddressFactory()
    other = AddressFactory()

    log = construct_log(session, address=address)

    params = FilterParams(address=(other, address))

    results = filter(session, params)

    for result in results:
        check_log_matches_filter(params, result)

    assert len(results) == 1
    assert results[0].id == log.id
    assert results[0].address == address


def test_filter_log_before_from_block(session):
    construct_log(session, block_number=0)

    params = FilterParams(from_block=1)

    results = filter(session, params)
    assert not results


def test_filter_log_after_to_block(session):
    construct_log(session, block_number=2)

    params = FilterParams(to_block=1)

    results = filter(session, params)
    assert not results
