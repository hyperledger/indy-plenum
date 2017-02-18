import pytest


@pytest.mark.skip(reason="SOV-547. Test incomplete")
def test_send_new_steward_txn(cli):
    assert cli.lastCmdOutput == "Genesis transaction added"
