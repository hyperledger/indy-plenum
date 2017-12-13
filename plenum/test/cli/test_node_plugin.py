import pytest


@pytest.mark.skipif('sys.platform == "win32"', reason='SOV-457')
def testEachNodeHasSeparatePluginObject(cli,
                                        validNodeNames,
                                        loadOpVerificationPlugin,
                                        createAllNodes):
    """
    This test is needed to make sure each node has a different plugin object
    loaded. This is neccessary for plugins that maintain some state
    """
    pluginObjIds = set()
    i = 0
    for node in cli.nodes.values():
        i += len(node.opVerifiers)
        for p in node.opVerifiers:
            pluginObjIds.add(id(p))

    assert i == len(pluginObjIds)
