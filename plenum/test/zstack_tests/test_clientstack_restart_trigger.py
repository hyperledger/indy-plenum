import pytest
import types

from plenum.common.stacks import ClientZStack

max_connected_clients_num = 10

@pytest.fixture(scope="module")
def tconf(tconf, request):
    old_track_connected_clients_num_enabled = tconf.TRACK_CONNECTED_CLIENTS_NUM_ENABLED
    old_client_stack_restart_enabled = tconf.CLIENT_STACK_RESTART_ENABLED
    old_max_connected_clients_num = tconf.MAX_CONNECTED_CLIENTS_NUM
    old_min_stack_restart_timeout = tconf.MIN_STACK_RESTART_TIMEOUT
    old_max_stack_restart_time_deviation = tconf.MAX_STACK_RESTART_TIME_DEVIATION
    tconf.TRACK_CONNECTED_CLIENTS_NUM_ENABLED = True
    tconf.CLIENT_STACK_RESTART_ENABLED = True
    tconf.MAX_CONNECTED_CLIENTS_NUM = max_connected_clients_num
    tconf.MIN_STACK_RESTART_TIMEOUT = 0
    tconf.MAX_STACK_RESTART_TIME_DEVIATION = 0

    def reset():
        tconf.TRACK_CONNECTED_CLIENTS_NUM_ENABLED = old_track_connected_clients_num_enabled
        tconf.CLIENT_STACK_RESTART_ENABLED = old_client_stack_restart_enabled
        tconf.MAX_CONNECTED_CLIENTS_NUM = old_max_connected_clients_num
        tconf.MIN_STACK_RESTART_TIMEOUT = old_min_stack_restart_timeout
        tconf.MAX_STACK_RESTART_TIME_DEVIATION = old_max_stack_restart_time_deviation

    request.addfinalizer(reset)
    return tconf

is_restarted = False

def new_restart(self):
    global is_restarted
    is_restarted = True

def patch_stack_restart(node):
    node.clientstack.restart = types.MethodType(new_restart, node.clientstack)

def revert_origin_back(node, orig_restart):
    node.clientstack.restart = types.MethodType(orig_restart, node.clientstack)

def test_clientstack_restart_not_triggered(tconf, create_node_and_not_start):
    node = create_node_and_not_start

    global is_restarted
    is_restarted = False

    orig_restart = ClientZStack.restart
    patch_stack_restart(node)

    node.clientstack.connected_clients_num = max_connected_clients_num - 1
    node.clientstack.handle_connections_limit()

    assert is_restarted is False

    revert_origin_back(node, orig_restart)

def test_clientstack_restart_triggered(tconf, create_node_and_not_start):
    node = create_node_and_not_start

    global is_restarted
    is_restarted = False

    orig_restart = ClientZStack.restart
    patch_stack_restart(node)

    node.clientstack.connected_clients_num = max_connected_clients_num
    node.clientstack.handle_connections_limit()

    assert is_restarted is True
    is_restarted = False

    node.clientstack.connected_clients_num = max_connected_clients_num + 1
    node.clientstack.handle_connections_limit()

    assert is_restarted is True

    revert_origin_back(node, orig_restart)

def test_clientstack_restart_trigger_delayed(tconf, looper, create_node_and_not_start):
    node = create_node_and_not_start

    global is_restarted
    is_restarted = False

    orig_restart = ClientZStack.restart
    patch_stack_restart(node)

    node.clientstack.min_stack_restart_timeout = 2

    node.clientstack.connected_clients_num = max_connected_clients_num + 1
    node.clientstack.handle_connections_limit()

    assert is_restarted is True
    is_restarted = False

    node.clientstack.connected_clients_num = max_connected_clients_num + 1
    node.clientstack.handle_connections_limit()

    assert is_restarted is False

    looper.runFor(node.clientstack.min_stack_restart_timeout + 0.5)

    node.clientstack.handle_connections_limit()
    assert is_restarted is True

    revert_origin_back(node, orig_restart)
