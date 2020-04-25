import secrets

from cthaeh.filter import filter, FilterParams
from cthaeh.tools.logs import construct_log, check_log_matches_filter


def test_filter_log_empty_params(session):
    log = construct_log(session)

    params = FilterParams()

    results = filter(session, params)

    for result in results:
        check_log_matches_filter(params, result)

    assert len(results) == 1
    assert results[0].id == log.id


def test_filter_log_single_address_match(session):
    log = construct_log(session)

    params = FilterParams(address=log.address)

    results = filter(session, params)

    for result in results:
        check_log_matches_filter(params, result)

    assert len(results) == 1
    assert results[0].id == log.id


def test_filter_log_multiple_addresses(session):
    other = secrets.token_bytes(20)

    log = construct_log(session)

    params = FilterParams(address=(other, log.address))

    results = filter(session, params)

    for result in results:
        check_log_matches_filter(params, result)

    assert len(results) == 1
    assert results[0].id == log.id
