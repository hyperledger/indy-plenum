import pytest

from plenum.common.messages.node_messages import InstanceChange
from plenum.server.models import InstanceChanges
from plenum.server.suspicion_codes import Suspicions
from plenum.test.helper import MockTimestamp


@pytest.fixture(scope="function")
def instance_changes(tconf):
    return InstanceChanges(tconf)


def test_instance_changes_are_empty_when_created(instance_changes):
    frm = "Node1"
    view_no = 1

    assert not instance_changes
    assert view_no not in instance_changes
    assert not instance_changes.has_view(view_no)
    assert not instance_changes.has_inst_chng_from(view_no, frm)


def test_add_first_vote(instance_changes):
    frm = "Node1"
    view_no = 1
    msg = InstanceChange(view_no, Suspicions.PRIMARY_DEGRADED.code)
    assert not instance_changes

    instance_changes.add_vote(msg, frm)

    assert instance_changes[view_no].msg == msg
    assert instance_changes[view_no].voters[frm]
    assert instance_changes.has_view(view_no)
    assert instance_changes.has_inst_chng_from(view_no, frm)


def test_equal_votes_dont_accumulate_when_added(instance_changes, tconf):
    frm = "Node1"
    view_no = 1
    time_provider = MockTimestamp(0)
    second_vote_time = 1
    instance_changes = InstanceChanges(tconf, time_provider)
    msg = InstanceChange(view_no, Suspicions.PRIMARY_DEGRADED.code)

    instance_changes.add_vote(msg, frm)
    time_provider.value = second_vote_time
    instance_changes.add_vote(msg, frm)

    assert instance_changes[view_no].voters[frm] == second_vote_time
    assert len(instance_changes[view_no].voters) == 1
    assert len(instance_changes) == 1


def test_too_old_messages_dont_count_towards_quorum(instance_changes, tconf):
    frm1 = "Node1"
    frm2 = "Node2"
    view_no = 1
    quorum = 2
    time_provider = MockTimestamp(0)
    instance_changes = InstanceChanges(tconf, time_provider)
    msg = InstanceChange(view_no, Suspicions.PRIMARY_DEGRADED.code)

    instance_changes.add_vote(msg, frm1)
    time_provider.value += (tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL/2)
    instance_changes.add_vote(msg, frm2)

    time_provider.value += (tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL/2) + 1
    assert not instance_changes.has_quorum(view_no, quorum)

    assert instance_changes.has_view(view_no)
    assert instance_changes[view_no].msg == msg
    assert not instance_changes.has_inst_chng_from(view_no, frm1)
    assert instance_changes.has_inst_chng_from(view_no, frm2)


def test_instance_changes_has_quorum_when_enough_distinct_votes_are_added(instance_changes):
    quorum = 2
    view_no = 1

    assert not instance_changes.has_quorum(view_no, quorum)
    for i in range(quorum):
        instance_changes.add_vote(InstanceChange(view_no, Suspicions.PRIMARY_DEGRADED.code),
                                  "Node{}".format(i))
    assert instance_changes.has_quorum(view_no, quorum)


def test_old_ic_discard(instance_changes, tconf):
    frm = "Node1"
    view_no = 1
    quorum = 2
    time_provider = MockTimestamp(0)
    instance_changes = InstanceChanges(tconf, time_provider)
    msg = InstanceChange(view_no, Suspicions.PRIMARY_DEGRADED.code)

    time_provider.value = 0
    instance_changes.add_vote(msg, frm)
    time_provider.value += tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL + 1
    assert not instance_changes.has_view(view_no)

    instance_changes.add_vote(msg, frm)
    time_provider.value += tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL + 1
    assert not instance_changes.has_inst_chng_from(view_no, frm)

    instance_changes.add_vote(msg, frm)
    time_provider.value += tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL + 1
    assert not instance_changes.has_quorum(view_no, quorum)
