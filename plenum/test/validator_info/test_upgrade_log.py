import os

import pytest


@pytest.fixture(scope='function')
def negative_log_size(tconf):
    old_size = tconf.VALIDATOR_INFO_UPGRADE_LOG_SIZE
    tconf.VALIDATOR_INFO_UPGRADE_LOG_SIZE = -1
    yield tconf
    tconf.VALIDATOR_INFO_UPGRADE_LOG_SIZE = old_size


@pytest.fixture(scope='function')
def huge_log_size(tconf):
    old_size = tconf.VALIDATOR_INFO_UPGRADE_LOG_SIZE
    tconf.VALIDATOR_INFO_UPGRADE_LOG_SIZE = 9999999
    yield tconf
    tconf.VALIDATOR_INFO_UPGRADE_LOG_SIZE = old_size


def test_validator_info_upgrade_log_crop(node, tconf):
    node.config.upgradeLogFile = 'upgrade_log'
    path_to_upgrade_log = os.path.join(os.path.join(node.ledger_dir,
                                                    node.config.upgradeLogFile))
    log_size = tconf.VALIDATOR_INFO_UPGRADE_LOG_SIZE
    with open(path_to_upgrade_log, 'w') as upgrade_log:
        upgrade_log.writelines(['foo\n'] * (log_size + 1))

    assert len(node._info_tool.extractions["Extractions"]["upgrade_log"]) == log_size


def test_validator_info_upgrade_log_ignore_negative(node, negative_log_size):
    assert negative_log_size.VALIDATOR_INFO_UPGRADE_LOG_SIZE == -1
    path_to_upgrade_log = os.path.join(os.path.join(node.ledger_dir,
                                                    node.config.upgradeLogFile))
    log_size = 10
    with open(path_to_upgrade_log, 'w') as upgrade_log:
        upgrade_log.writelines(['foo\n'] * (log_size))

    assert len(node._info_tool.extractions["Extractions"]["upgrade_log"]) == log_size


def test_validator_info_upgrade_log_ignore_huge(node, huge_log_size):
    assert huge_log_size.VALIDATOR_INFO_UPGRADE_LOG_SIZE == 9999999
    path_to_upgrade_log = os.path.join(os.path.join(node.ledger_dir,
                                                    node.config.upgradeLogFile))
    log_size = 10
    with open(path_to_upgrade_log, 'w') as upgrade_log:
        upgrade_log.writelines(['foo\n'] * (log_size))

    assert len(node._info_tool.extractions["Extractions"]["upgrade_log"]) == log_size
