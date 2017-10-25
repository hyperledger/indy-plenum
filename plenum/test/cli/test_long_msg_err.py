from plenum.test.cli.helper import createClientAndConnect

import pytest
from stp_core.common.log import Logger


def test_error_if_long_message(
        cli, tconf, createAllNodes, validNodeNames, set_info_log_level):
    operation = '{{"Hello": "{}"}}'.format("T" * tconf.MSG_LEN_LIMIT)
    createClientAndConnect(cli, validNodeNames, "Alice")

    cli.enterCmd('client {} send {}'.format("Alice", operation))
    assert "message is too large and cannot be split" in cli.lastCmdOutput
