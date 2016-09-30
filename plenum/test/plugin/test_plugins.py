import os

import pytest

from plenum.common.types import PLUGIN_TYPE_VERIFICATION
from plenum.server.plugin_loader import PluginLoader

curPath = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture
def plugin():
    pl = PluginLoader(os.path.join(curPath, 'name_age_verification'))
    verPlugins = pl.plugins[PLUGIN_TYPE_VERIFICATION]
    assert len(verPlugins) == 1
    p, = verPlugins
    assert p.__class__.__name__ == 'NameAndAgeVerifier'
    return p


def testValidOperations(plugin):
    operations = [
        {"name": "Alice", "age": 27},
        {"name": "Susan", "age": 0},
        {"name": "John", "age": "47"}
    ]

    for operation in operations:
        plugin.verify(operation)


def testInvalidOperations(plugin):
    operations = [
        {"name": "Alice has a very long name, too long in fact", "age": 27},
        {"name": "Susan", "age": -1},
        {"name": "John", "age": "forty-seven"}
    ]

    for operation in operations:
        with pytest.raises(Exception):
            assert plugin.plugin_object.checkValidOperation(operation)


def testPluginEmptyPath():
    with pytest.raises(AssertionError) as exInfo:
        PluginLoader("")
    assert 'path is required' == str(exInfo.value)


def testPluginWrongPath():
    with pytest.raises(FileNotFoundError):
        pl = PluginLoader("somerandompath")
