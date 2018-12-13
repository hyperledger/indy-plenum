import pytest

from plenum.common.messages.node_messages import InstanceChange
from plenum.server.models import InstanceChanges
from plenum.server.suspicion_codes import Suspicions
from stp_core.loop.eventually import eventually


@pytest.fixture(scope="function")
def instance_changes(tconf):
    return InstanceChanges(tconf)


@pytest.fixture(scope="module")
def tconf(tconf):
    old_interval = tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL
    tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL = 10
    yield tconf

    tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL = old_interval


def test_add_vote(instance_changes):
    frm = "Node1"
    view_no = 1
    msg = InstanceChange(view_no, Suspicions.PRIMARY_DEGRADED.code)
    instance_changes.add_vote(msg, frm)
    assert instance_changes[view_no].msg == msg
    assert instance_changes[view_no].voters[frm]


def test_has_view(instance_changes):
    frm = "Node1"
    view_no = 1
    msg = InstanceChange(view_no, Suspicions.PRIMARY_DEGRADED.code)
    instance_changes.add_vote(msg, frm)
    assert instance_changes.has_view(view_no)


def test_has_inst_chng_from(instance_changes):
    frm = "Node1"
    view_no = 1
    msg = InstanceChange(view_no, Suspicions.PRIMARY_DEGRADED.code)
    instance_changes.add_vote(msg, frm)
    assert instance_changes.has_inst_chng_from(view_no, frm)


def test_has_quorum(instance_changes):
    quorum = 2
    view_no = 1

    assert not instance_changes.has_quorum(view_no, quorum)
    for i in range(quorum):
        instance_changes.add_vote(InstanceChange(view_no, Suspicions.PRIMARY_DEGRADED.code),
                                  "Node{}".format(i))
    assert instance_changes.has_quorum(view_no, quorum)


def test_old_ic_discard(instance_changes, looper, tconf):
    frm = "Node1"
    view_no = 1
    quorum = 2
    msg = InstanceChange(view_no, Suspicions.PRIMARY_DEGRADED.code)
    instance_changes.add_vote(msg, frm)

    def chk_ic_discard():
        assert not instance_changes.has_view(view_no)
        assert not instance_changes.has_inst_chng_from(view_no, frm)
        assert not instance_changes.has_quorum(view_no, quorum)
    looper.run(eventually(chk_ic_discard,
                          timeout=tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL))
