from plenum.common.messages.internal_messages import VoteForViewChange, PrimarySelected, NodeStatusUpdated
from plenum.common.startable import Mode, Status
from plenum.test.helper import TestStopwatch, TestInternalBus


def num_votes_for_view_change(bus: TestInternalBus):
    return sum(1 for m in bus.sent_messages if isinstance(m, VoteForViewChange))


def test_instance_change_on_primary_disconnected(tconf, timer, internal_bus, external_bus,
                                                 primary_connection_monitor_service):
    stopwatch = TestStopwatch(timer)

    # Primary disconnected
    primary_name = primary_connection_monitor_service._data.primary_name
    external_bus.disconnect(primary_name)

    # Check that there are no votes for view changes
    assert num_votes_for_view_change(internal_bus) == 0

    # Check first instance change is sent after ToleratePrimaryDisconnection timeout
    stopwatch.start()
    timer.wait_for(lambda: num_votes_for_view_change(internal_bus) == 1)
    assert stopwatch.has_elapsed(tconf.ToleratePrimaryDisconnection)

    # Check as long as primary is disconnected next instance changes are sent with NEW_VIEW_TIMEOUT period
    for i in range(1, 6):
        stopwatch.start()
        timer.wait_for(lambda: num_votes_for_view_change(internal_bus) == 1 + i)
        assert stopwatch.has_elapsed(tconf.NEW_VIEW_TIMEOUT)

    # Primary connected
    external_bus.connect(primary_name)
    assert num_votes_for_view_change(internal_bus) == 6

    # Check new votes for view change are never sent
    timer.run_to_completion()
    assert num_votes_for_view_change(internal_bus) == 6


def test_instance_changes_are_not_sent_while_not_synced(tconf, timer, internal_bus, external_bus,
                                                        primary_connection_monitor_service):
    primary_connection_monitor_service._data.node_mode = Mode.syncing

    # Primary disconnected
    primary_name = primary_connection_monitor_service._data.primary_name
    external_bus.disconnect(primary_name)

    # Check that there are no votes for view changes
    assert num_votes_for_view_change(internal_bus) == 0

    # Check that there are no votes for view change after quite a lot of time
    timer.run_for(10*(tconf.ToleratePrimaryDisconnection + tconf.NEW_VIEW_TIMEOUT))
    assert num_votes_for_view_change(internal_bus) == 0

    # Check that instance change is sent after node becomes synced
    primary_connection_monitor_service._data.node_mode = Mode.synced
    timer.wait_for(lambda: num_votes_for_view_change(internal_bus) == 1)


def test_instance_changes_are_not_sent_if_primary_is_unknown(tconf, timer, internal_bus, external_bus,
                                                             primary_connection_monitor_service):
    # Primary disconnected
    primary_connection_monitor_service._data.primary_name = "UnknownOmega"
    internal_bus.send(PrimarySelected())

    # Check that there are no votes for view change after quite a lot of time
    timer.run_for(10*(tconf.ToleratePrimaryDisconnection + tconf.NEW_VIEW_TIMEOUT))
    assert num_votes_for_view_change(internal_bus) == 0


def test_instance_changes_are_not_sent_if_primary_disconnected_for_short_time(tconf, timer, internal_bus, external_bus,
                                                                              primary_connection_monitor_service):
    # Primary disconnected
    primary_name = primary_connection_monitor_service._data.primary_name
    external_bus.disconnect(primary_name)

    # Check that there are no votes for view changes
    assert num_votes_for_view_change(internal_bus) == 0

    # Wait for almost up to timeout
    timer.run_for(0.9 * tconf.ToleratePrimaryDisconnection)

    # Check that there are no votes for view changes
    assert num_votes_for_view_change(internal_bus) == 0

    # Primary connected
    external_bus.connect(primary_name)

    # Check that there won't be any votes for view change
    timer.run_to_completion()
    assert num_votes_for_view_change(internal_bus) == 0


def test_instance_changes_are_not_sent_after_entering_view_change(tconf, timer, internal_bus, external_bus,
                                                                  primary_connection_monitor_service):
    validators = primary_connection_monitor_service._data.validators
    primary_name = primary_connection_monitor_service._data.primary_name
    next_primary_name = next(name for name in validators if name != primary_name)

    # Primary disconnected
    external_bus.disconnect(primary_name)

    # Check that there are no votes for view changes
    assert num_votes_for_view_change(internal_bus) == 0

    # Check first instance change is sent after ToleratePrimaryDisconnection timeout
    timer.wait_for(lambda: num_votes_for_view_change(internal_bus) == 1)

    # Check that there won't be any new votes for view change
    primary_connection_monitor_service._data.waiting_for_new_view = True
    primary_connection_monitor_service._data.primary_name = next_primary_name
    internal_bus.send(PrimarySelected())
    timer.run_to_completion()
    assert num_votes_for_view_change(internal_bus) == 1


def test_instance_changes_are_sent_after_selecting_disconnected_primary(tconf, timer, internal_bus, external_bus,
                                                                        primary_connection_monitor_service):
    validators = primary_connection_monitor_service._data.validators
    primary_name = primary_connection_monitor_service._data.primary_name
    next_primary_name = next(name for name in validators if name != primary_name)

    # Next primary disconnected
    external_bus.disconnect(next_primary_name)

    # Check that there are no votes for view changes
    timer.run_to_completion()
    assert num_votes_for_view_change(internal_bus) == 0

    # Emulate view change
    primary_connection_monitor_service._data.waiting_for_new_view = True
    primary_connection_monitor_service._data.primary_name = next_primary_name
    internal_bus.send(PrimarySelected())

    # Check that vote for view change will be sent
    timer.wait_for(lambda: num_votes_for_view_change(internal_bus) == 1)


def test_instance_changes_are_sent_after_going_back_to_started_state(tconf, timer, internal_bus, external_bus,
                                                                     primary_connection_monitor_service):
    primary_connection_monitor_service._data.node_status = Status.starting
    primary_connection_monitor_service._data.node_mode = Mode.starting

    # Primary disconnected
    primary_name = primary_connection_monitor_service._data.primary_name
    external_bus.disconnect(primary_name)

    # Check that there are no votes for view change after quite a lot of time
    timer.run_for(10*(tconf.ToleratePrimaryDisconnection + tconf.NEW_VIEW_TIMEOUT))
    assert num_votes_for_view_change(internal_bus) == 0

    # Check that after connecting to enough nodes vote for view change will be sent
    primary_connection_monitor_service._data.node_mode = Mode.synced
    primary_connection_monitor_service._data.node_status = Status.started_hungry
    # TODO: Do we really need this message? Test successfully passes even without this message
    # internal_bus.send(NodeStatusUpdated(old_status=Status.starting, new_status=Status.started_hungry))
    timer.wait_for(lambda: num_votes_for_view_change(internal_bus) == 1)
