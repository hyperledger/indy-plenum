from plenum.test.cli.helper import createClientAndConnect

import pytest
from stp_core.common.log import Logger

@pytest.fixture(scope="function")
def patch_msg_len(tconf):
    old_value = tconf.MSG_LEN_LIMIT
    tconf.MSG_LEN_LIMIT = 128 * 1024
    yield tconf.MSG_LEN_LIMIT
    print(old_value)
    tconf.MSG_LEN_LIMIT = old_value

def test_error_if_long_message(
        patch_msg_len, cli, tconf, createAllNodes, validNodeNames, set_info_log_level):
    operation = '{{"Hello": "{}"}}'.format("T" * tconf.MSG_LEN_LIMIT)
    createClientAndConnect(cli, validNodeNames, "Alice")

    cli.enterCmd('client {} send {}'.format("Alice", operation))
    assert "Message will be discarded due to InvalidMessageExceedingSizeException" in cli.lastCmdOutput
