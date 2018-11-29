from contextlib import contextmanager

import pytest
import functools

from plenum.server.instances import Instances
from plenum.test.testing_utils import FakeSomething


@pytest.fixture(scope="function")
def fake_monitor():
    monitor = FakeSomething(
    )
    return monitor


@pytest.fixture(scope='function', params=[0, 10])
def fake_node(fake_node):
    @contextmanager
    def measure_time(*args):
        yield 
    fake_node.instances = Instances()
    fake_node.instances.add(0)
    fake_node.spylog = []
    fake_node.metrics = FakeSomething(add_event=lambda *args: True,
                                      measure_time=measure_time)
    return fake_node


def test_not_send_IC_during_view_change(fake_node,
                                        testNodeClass,
                                        fake_monitor):
    fake_node.view_change_in_progress = True
    fake_node.isParticipating = True
    fake_node.checkPerformance = functools.partial(testNodeClass.checkPerformance, fake_node)
    assert fake_node.checkPerformance() == None


def test_send_IC_if_master_degraded(fake_node,
                                    testNodeClass):
    fake_node.isParticipating = True
    fake_node.view_change_in_progress = False
    fake_node._update_new_ordered_reqs_count = lambda: True
    fake_node.sendNodeRequestSpike = lambda: True
    fake_monitor.isMasterDegraded = lambda: True
    fake_monitor.areBackupsDegraded = lambda: []
    fake_monitor.getThroughputs = lambda a: (None, None)
    fake_monitor.getLatencies = lambda: (None, None)
    fake_monitor.getLatency = lambda a: 0.0
    fake_node.view_changer.on_master_degradation = lambda: True
    fake_node.monitor = fake_monitor
    fake_node.checkPerformance = functools.partial(testNodeClass.checkPerformance, fake_node)
    assert fake_node.checkPerformance() == False


def test_not_send_IC_if_not_isParticipating(fake_node,
                                            testNodeClass):
    fake_node.isParticipating = False
    fake_node.checkPerformance = functools.partial(testNodeClass.checkPerformance, fake_node)
    assert fake_node.checkPerformance() == None


def test_not_send_if_not_new_ordered_reqs(fake_node,
                                          testNodeClass):
    fake_node.isParticipating = True
    fake_node.view_change_in_progress = False
    fake_node._update_new_ordered_reqs_count = lambda: False
    fake_node.checkPerformance = functools.partial(testNodeClass.checkPerformance, fake_node)
    assert fake_node.checkPerformance() == None


def test_not_send_if_master_Id_is_None(fake_node,
                                      testNodeClass):
    fake_node.isParticipating = True
    fake_node.view_change_in_progress = False
    fake_node._update_new_ordered_reqs_count = lambda: True
    fake_node.instances = Instances()
    fake_node.checkPerformance = functools.partial(testNodeClass.checkPerformance, fake_node)
    assert fake_node.checkPerformance() == True
