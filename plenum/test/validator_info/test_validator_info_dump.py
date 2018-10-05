import os
import pytest

from plenum.server.node import Node


@pytest.fixture()
def validator_use_db_patched(tconf_for_func, request):
    old_val = tconf_for_func.VALIDATOR_INFO_USE_DB
    tconf_for_func.VALIDATOR_INFO_USE_DB = True

    def reset():
        tconf_for_func.VALIDATOR_INFO_USE_DB = old_val

    request.addfinalizer(reset)
    return tconf_for_func


def test_dump_general_info_use_db(validator_use_db_patched, node):
    node._info_tool.dump_general_info()
    file_name = node._info_tool.GENERAL_FILE_NAME_TEMPLATE.format(node_name=node.name.lower())
    file_path = os.path.join(node.node_info_dir, file_name)
    db_name = node._info_tool.GENERAL_DB_NAME_TEMPLATE.format(node_name=node.name.lower())
    db_path = os.path.join(node.node_info_dir, db_name)
    assert os.path.exists(file_path)
    assert os.path.exists(db_path)


def test_dump_additional_info(node):
    Node.dump_additional_info(node)
    file_name = node._info_tool.ADDITIONAL_FILE_NAME_TEMPLATE.format(node_name=node.name.lower())
    file_path = os.path.join(node.node_info_dir, file_name)
    assert os.path.exists(file_path)
