from plenum.test.test_node import TestNode, genNodeReg
from copy import copy
import psutil


# noinspection PyIncorrectDocstring
def testSystemStats(monkeypatch, tdir):
    """
    Checking if monitor captures system performance data
    """
    cpu = 10
    ram = 15
    bytes = 1024

    class TestRam():
        def __init__(self):
            nonlocal ram
            self.percent = ram

    class TestBytes():
        def __init__(self):
            nonlocal bytes
            self.bytes_sent = bytes
            self.bytes_recv = 0

    def test_cpu_percent(interval):
        nonlocal cpu
        return cpu

    def test_virtual_memory():
        return TestRam()

    def test_traffic():
        return TestBytes()

    name = 'TestNodeMonitorPerfStats'
    nodeReg = genNodeReg(names=[name])
    ha, cliname, cliha = nodeReg[name]
    node = TestNode(name=name, ha=ha, cliname=cliname, cliha=cliha, nodeRegistry=copy(nodeReg), basedirpath=tdir,
                    primaryDecider=None, pluginPaths=None)
    monkeypatch.setattr(psutil, 'cpu_percent', test_cpu_percent)
    monkeypatch.setattr(psutil, 'virtual_memory', test_virtual_memory)
    monkeypatch.setattr(psutil, 'net_io_counters', test_traffic)
    node.monitor.lastKnownTraffic = 0
    data1 = node.monitor.captureSystemPerformance()
    assert 'cpu' in data1
    assert 'ram' in data1
    assert 'traffic' in data1
    assert data1['cpu']['value'] == cpu
    assert data1['ram']['value'] == ram
    assert data1['traffic']['value'] == bytes / 1024
    cpu = 50
    ram = 60
    bytes = 2048
    assert node.monitor.lastKnownTraffic == data1['traffic']['value']
    data2 = node.monitor.captureSystemPerformance()
    assert data2['cpu']['value'] == cpu
    assert data2['ram']['value'] == ram
    assert data2['traffic']['value'] == bytes / 1024 - data1['traffic']['value']