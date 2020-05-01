import pytest

from cthaeh.exceptions import NoGapFound
from cthaeh.loader import (
    find_first_missing_block_number,
    find_first_detatched_block_after,
)
from cthaeh.tools.factories import Hash32Factory, HeaderFactory


def test_find_first_missing_block_number(session):
    with session.begin_nested():
        genesis = HeaderFactory(block_number=0)
        block_1 = HeaderFactory(block_number=1, parent_hash=genesis.hash)
        HeaderFactory(block_number=2, parent_hash=block_1.hash)

        block_6 = HeaderFactory(
            block_number=6,
            parent_hash=None,
            detatched_parent_hash=Hash32Factory(),
        )
        HeaderFactory(block_number=7, parent_hash=block_6.hash)

        block_12 = HeaderFactory(
            block_number=12,
            parent_hash=None,
            detatched_parent_hash=Hash32Factory(),
        )
        HeaderFactory(block_number=13, parent_hash=block_12.hash)

    first_missing_number = find_first_missing_block_number(session)
    assert first_missing_number == 3


def test_find_first_detatched_block_after(session):
    with session.begin_nested():
        genesis = HeaderFactory(block_number=0)
        block_1 = HeaderFactory(block_number=1, parent_hash=genesis.hash)
        HeaderFactory(block_number=2, parent_hash=block_1.hash)

        block_6 = HeaderFactory(
            block_number=6,
            parent_hash=None,
            detatched_parent_hash=Hash32Factory(),
        )
        HeaderFactory(block_number=7, parent_hash=block_6.hash)

        block_12 = HeaderFactory(
            block_number=12,
            parent_hash=None,
            detatched_parent_hash=Hash32Factory(),
        )
        HeaderFactory(block_number=13, parent_hash=block_12.hash)

    gap_0_upper_bound = find_first_detatched_block_after(session, after_height=3)
    assert gap_0_upper_bound == 6

    gap_1_upper_bound = find_first_detatched_block_after(session, after_height=8)
    assert gap_1_upper_bound == 12

    with pytest.raises(NoGapFound):
        find_first_detatched_block_after(session, after_height=12)
    with pytest.raises(NoGapFound):
        find_first_detatched_block_after(session, after_height=13)
