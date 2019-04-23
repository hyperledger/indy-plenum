import time

import pytest


# TODO: Turn this into ViewChangerNodeDataProvider tests


@pytest.fixture(params=[0])
def fake_view_changer(fake_view_changer):
    return fake_view_changer


def test_is_primary_disconnected_lost_none(fake_view_changer):
    fake_view_changer.node.primaries_disconnection_times[0] = None
    assert not fake_view_changer.provider.is_primary_disconnected()


def test_is_primary_disconnected_primary_none(fake_view_changer):
    fake_view_changer.node.primaries_disconnection_times[0] = time.perf_counter()
    fake_view_changer.node.master_primary_name = None
    assert not fake_view_changer.provider.is_primary_disconnected()


def test_is_primary_disconnected_primary_absent(fake_view_changer):
    fake_view_changer.node.primaries_disconnection_times[0] = time.perf_counter()
    fake_view_changer.node.master_primary_name = 'Alpha'
    assert not fake_view_changer.provider.is_primary_disconnected()


def test_is_primary_disconnected(fake_view_changer):
    fake_view_changer.node.primaries_disconnection_times[0] = time.perf_counter()
    fake_view_changer.node.master_primary_name = 'Alpha'
    fake_view_changer.node.nodestack.conns.remove('Alpha')
    assert fake_view_changer.provider.is_primary_disconnected()
