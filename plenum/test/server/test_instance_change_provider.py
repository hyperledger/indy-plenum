import os
import pytest as pytest

from common.serializers.serialization import node_status_db_serializer
from plenum.test.helper import MockTimestamp
from plenum.common.messages.node_messages import InstanceChange
from plenum.server.suspicion_codes import Suspicions
from plenum.server.view_change.instance_change_provider import InstanceChangeProvider
from storage.helper import initKeyValueStorage
from plenum.test.logging.conftest import logsearch


@pytest.fixture(scope="function")
def time_provider():
    return MockTimestamp(0)


@pytest.fixture(scope="function")
def node_status_db(tconf):
    data_location = tconf.GENERAL_CONFIG_DIR + "/node_status_db"
    if not os.path.isdir(data_location):
        os.makedirs(data_location)
    node_status_db = initKeyValueStorage(tconf.nodeStatusStorage,
                                         data_location,
                                         tconf.nodeStatusDbName,
                                         db_config=tconf.db_node_status_db_config)
    yield node_status_db
    node_status_db.drop()


@pytest.fixture(scope="function")
def instance_change_provider(tconf, node_status_db, time_provider):
    return InstanceChangeProvider(tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL,
                                  node_status_db,
                                  time_provider)


def test_add_first_vote(instance_change_provider):
    frm = "Node1"
    view_no = 1
    msg = InstanceChange(view_no, Suspicions.PRIMARY_DEGRADED.code)

    assert not instance_change_provider.has_view(view_no)
    assert not instance_change_provider.has_inst_chng_from(view_no, frm)

    instance_change_provider.add_vote(msg, frm)

    assert instance_change_provider.has_view(view_no)
    assert instance_change_provider.has_inst_chng_from(view_no, frm)
    assert _is_view_in_db(view_no, instance_change_provider)


def test_old_ic_discard(instance_change_provider, tconf, time_provider):
    frm = "Node1"
    view_no = 1
    quorum = 2
    msg = InstanceChange(view_no, Suspicions.PRIMARY_DEGRADED.code)

    time_provider.value = 0
    instance_change_provider.add_vote(msg, frm)
    time_provider.value += tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL + 1
    assert not instance_change_provider.has_view(view_no)

    instance_change_provider.add_vote(msg, frm)
    time_provider.value += tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL + 1
    assert not instance_change_provider.has_inst_chng_from(view_no, frm)

    instance_change_provider.add_vote(msg, frm)
    time_provider.value += tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL + 1
    assert not instance_change_provider.has_quorum(view_no, quorum)


def test_equal_votes_dont_accumulate_when_added(instance_change_provider,
                                                time_provider):
    frm = "Node1"
    view_no = 1
    quorum = 2
    time_provider.value = 0
    second_vote_time = 1
    msg = InstanceChange(view_no, Suspicions.PRIMARY_DEGRADED.code)

    instance_change_provider.add_vote(msg, frm)
    time_provider.value = second_vote_time
    instance_change_provider.add_vote(msg, frm)

    assert instance_change_provider.has_view(view_no)
    assert instance_change_provider.has_inst_chng_from(view_no, frm)
    assert not instance_change_provider.has_quorum(view_no, quorum)
    instance_changes_db = instance_change_provider._node_status_db.get(
        instance_change_provider.generate_db_key(view_no))
    assert len(node_status_db_serializer.deserialize(instance_changes_db)) == 1


def test_too_old_messages_dont_count_towards_quorum(instance_change_provider,
                                                    time_provider, tconf):
    frm1 = "Node1"
    frm2 = "Node2"
    view_no = 1
    quorum = 2
    time_provider.value = 0
    msg = InstanceChange(view_no, Suspicions.PRIMARY_DEGRADED.code)

    instance_change_provider.add_vote(msg, frm1)
    time_provider.value += (tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL / 2)
    instance_change_provider.add_vote(msg, frm2)
    assert instance_change_provider.has_quorum(view_no, quorum)

    time_provider.value += (tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL / 2) + 1
    assert not instance_change_provider.has_quorum(view_no, quorum)

    assert not instance_change_provider.has_inst_chng_from(view_no, frm1)
    assert instance_change_provider.has_inst_chng_from(view_no, frm2)
    instance_changes_db = instance_change_provider._node_status_db.get(
        instance_change_provider.generate_db_key(view_no))
    assert len(node_status_db_serializer.deserialize(instance_changes_db)) == 1


def test_instance_changes_has_quorum_when_enough_distinct_votes_are_added(instance_change_provider):
    quorum = 2
    view_no = 1

    assert not instance_change_provider.has_quorum(view_no, quorum)
    for i in range(quorum):
        instance_change_provider.add_vote(InstanceChange(view_no, Suspicions.PRIMARY_DEGRADED.code),
                                          "Node{}".format(i))
    assert instance_change_provider.has_quorum(view_no, quorum)


def test_update_instance_changes_in_db(instance_change_provider, tconf, node_status_db, time_provider):
    frm = "Node1"
    view_no = 1
    msg = InstanceChange(view_no, Suspicions.PRIMARY_DEGRADED.code)

    assert not instance_change_provider.has_view(view_no)
    assert not instance_change_provider.has_inst_chng_from(view_no, frm)
    assert not _is_view_in_db(view_no, instance_change_provider)

    instance_change_provider.add_vote(msg, frm)
    assert instance_change_provider.has_view(view_no)
    assert instance_change_provider.has_inst_chng_from(view_no, frm)
    assert _is_view_in_db(view_no, instance_change_provider)

    instance_change_provider._node_status_db.close()
    assert instance_change_provider._node_status_db.closed
    instance_change_provider._node_status_db.open()

    new_instance_change_provider = InstanceChangeProvider(tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL,
                                                          node_status_db, time_provider)
    assert new_instance_change_provider.has_view(view_no)
    assert new_instance_change_provider.has_inst_chng_from(view_no, frm)


def test_fail_update_instance_changes_from_db(instance_change_provider, tconf,
                                              node_status_db, time_provider,
                                              logsearch):
    # test updating cache with view without votes
    node_status_db.iterator = lambda include_value=True: {
        "3": node_status_db_serializer.serialize(None)}.items()
    provider = InstanceChangeProvider(tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL,
                                      node_status_db, time_provider)
    assert not provider.has_view(3)

    # test updating cache with Vote with incorrect timestamp format
    node_status_db.iterator = lambda include_value=True: {
        "3": node_status_db_serializer.serialize({"voter": ["a", 10.4]})}.items()
    logs, _ = logsearch(
        msgs=["InstanceChangeProvider: timestamp in Vote .* : .* - .* must "
              "be of float or int type"])
    InstanceChangeProvider(tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL,
                           node_status_db, time_provider)
    assert logs

    # test updating cache with Vote with incorrect reason format
    node_status_db.iterator = lambda include_value=True: {
        "3": node_status_db_serializer.serialize({"voter": [5, 10.4]})}.items()
    logs, _ = logsearch(
        msgs=["InstanceChangeProvider: reason in Vote .* : .* - .* must "
              "be of int type"])
    InstanceChangeProvider(tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL,
                           node_status_db, time_provider)
    assert logs

    # test updating cache with incorrect view_no format
    node_status_db.iterator = lambda include_value=True: {
        "a": node_status_db_serializer.serialize({"voter": [5, 25]})}.items()
    logs, _ = logsearch(
        msgs=["InstanceChangeProvider: view_no='.*' "
              "must be of int type"])
    InstanceChangeProvider(tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL,
                           node_status_db, time_provider)
    assert logs

    # test updating cache with incorrect and correct view_no format
    node_status_db.iterator = lambda include_value=True: {
        "a": node_status_db_serializer.serialize({"voter1": [5, 25]}),
        "1": node_status_db_serializer.serialize({"voter2": [4, 15]})}.items()
    logs, _ = logsearch(
        msgs=["InstanceChangeProvider: view_no='.*' "
              "must be of int type"])
    provider = InstanceChangeProvider(tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL,
                                      node_status_db, time_provider)
    assert provider.has_view(1)
    assert provider.has_inst_chng_from(1, "voter2")
    assert logs


def test_remove_view(instance_change_provider):
    frm = "Node1"
    view_no = 2

    instance_change_provider.add_vote(InstanceChange(view_no - 1,
                                                     Suspicions.PRIMARY_DEGRADED.code), frm)
    instance_change_provider.add_vote(InstanceChange(view_no,
                                                     Suspicions.PRIMARY_DEGRADED.code), frm)

    assert instance_change_provider.has_view(view_no - 1)
    assert instance_change_provider.has_view(view_no)
    assert instance_change_provider.has_inst_chng_from(view_no - 1, frm)
    assert instance_change_provider.has_inst_chng_from(view_no, frm)
    assert _is_view_in_db(view_no, instance_change_provider)

    instance_change_provider.remove_view(view_no)

    assert not instance_change_provider.has_view(view_no - 1)
    assert not instance_change_provider.has_view(view_no)
    assert not instance_change_provider.has_inst_chng_from(view_no - 1, frm)
    assert not instance_change_provider.has_inst_chng_from(view_no, frm)
    assert not _is_view_in_db(view_no, instance_change_provider)


def _is_view_in_db(view_no, instance_change_provider):
    return instance_change_provider.generate_db_key(view_no) in instance_change_provider._node_status_db
