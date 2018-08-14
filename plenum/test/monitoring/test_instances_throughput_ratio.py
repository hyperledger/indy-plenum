from collections import namedtuple
from itertools import chain

import pytest

from plenum.server.monitor import ThroughputMeasurement


class ReqStream:
    Period = namedtuple('Period', ['start', 'interval', 'quantity'])
    Once = namedtuple('Once', ['time', 'quantity'])
    Stop = namedtuple('Stop', ['time'])

    def __init__(self):
        self._steps = []

    def period(self, s, i, q):
        self._steps.append(ReqStream.Period(start=s, interval=i, quantity=q))
        return self

    def once(self, t, q):
        self._steps.append(ReqStream.Once(time=t, quantity=q))
        return self

    def stop(self, t):
        self._steps.append(ReqStream.Stop(time=t))
        return self

    def build(self):
        sections = []
        for i in range(len(self._steps)):
            if isinstance(self._steps[i], ReqStream.Period):
                sections.append(
                    self._translate_period(self._steps[i],
                                           next_step=self._steps[i + 1]))
            elif isinstance(self._steps[i], ReqStream.Once):
                sections.append(self._translate_once(self._steps[i]))
            elif isinstance(self._steps[i], ReqStream.Stop):
                if not isinstance(self._steps[i - 1], ReqStream.Period):
                    raise RuntimeError('ReqStream Stop step is not'
                                       ' after Period step')
            else:
                raise RuntimeError('ReqStream step type is unsupported')
        return chain(*sections)

    @staticmethod
    def _translate_period(period, next_step):
        if isinstance(next_step, ReqStream.Period):
            end = next_step.start
        elif isinstance(next_step, ReqStream.Once) \
                or isinstance(next_step, ReqStream.Stop):
            end = next_step.time
        else:
            raise RuntimeError('ReqStream step type is unsupported')
        return ((ts, period.quantity)
                for ts in range(period.start, end, period.interval))

    @staticmethod
    def _translate_once(once):
        return [(once.time, once.quantity)]


@pytest.mark.parametrize('inst_req_streams, expected_is_master_degraded', [
    pytest.param([ReqStream().period(s=0, i=5, q=1)
                             .stop(t=4 * 60)
                             .build()
                  for inst_id in range(9)],
                 False,
                 id='master_not_degraded_if_same_throughput'),
    pytest.param([ReqStream().period(s=0, i=5, q=1)
                             .stop(t=1 * 60 * 60)
                             .build()]
                 + [ReqStream().period(s=0, i=5, q=1)
                               .once(t=1 * 60 * 60, q=1000)
                               .build()
                    for inst_id in range(1, 9)],
                 False,
                 id='master_not_degraded_on_spike_in_1_batch_on_backups'),
    pytest.param([ReqStream().period(s=0, i=5, q=1)
                             .stop(t=1 * 60 * 60)
                             .build()]
                 + [ReqStream().period(s=0, i=5, q=1)
                               .period(s=1 * 60 * 60 - 2, i=1, q=1000)
                               .stop(t=1 * 60 * 60)
                               .build()
                    for inst_id in range(1, 9)],
                 False,
                 id='master_not_degraded_on_spike'
                    '_in_2_batches_in_1_window_on_backups'),
    pytest.param([ReqStream().period(s=0, i=5, q=1)
                             .stop(t=1 * 60 * 60)
                             .build()]
                 + [ReqStream().period(s=0, i=5, q=1)
                               .period(s=1 * 60 * 60 - 1, i=1, q=1000)
                               .stop(t=1 * 60 * 60 + 1)
                               .build()
                    for inst_id in range(1, 9)],
                 True,
                 id='master_degraded_on_spike'
                    '_in_2_batches_in_2_windows_on_backups'),
    pytest.param([ReqStream().period(s=0, i=1, q=11)
                             .stop(t=4 * 60 * 60)
                             .build()]
                 + [ReqStream().period(s=0, i=1, q=11)
                               .stop(t=4 * 60 * 60 + 5 * 60)
                               .build()
                    for inst_id in range(1, 9)],
                 True,
                 id='master_degraded_on_stop_ordering_on_master'),
    pytest.param([ReqStream().period(s=0, i=1, q=11)
                             .stop(t=4 * 60 * 60 + 15 * 60)
                             .build()
                    for inst_id in range(0, 8)]
                 + [ReqStream().period(s=0, i=1, q=11)
                               .stop(t=4 * 60 * 60)
                               .once(t=4 * 60 * 60 + 15 * 60, q=9900)
                               .build()],
                 False,
                 id='master_not_degraded_on_queuing_reqs'
                    '_and_ordering_at_once_on_one_backup'),
    pytest.param([ReqStream().period(s=0, i=1, q=15)
                 .stop(t=4 * 60 * 60 + 11 * 60)
                 .build()
                  for inst_id in range(0, 8)]
                 + [ReqStream().period(s=0, i=1, q=15)
                 .stop(t=4 * 60 * 60)
                 .once(t=4 * 60 * 60 + 17 * 60, q=9900)
                 .build()],
                 False,
                 id='master_not_degraded_on_queuing_reqs'
                    '_and_ordering_at_once_on_one_backup'
                    '_while_load_stopped_in_meantime'),
])
def test_instances_throughput_ratio(inst_req_streams,
                                    expected_is_master_degraded,
                                    tconf):

    # print('DELTA = {}'.format(tconf.DELTA))
    # print('ThroughputInnerWindowSize = {}'
    #       .format(tconf.ThroughputInnerWindowSize))
    # print('ThroughputMinActivityThreshold = {}'
    #       .format(tconf.ThroughputMinActivityThreshold))
    # print('Max3PCBatchSize = {}'.format(tconf.Max3PCBatchSize))
    # print('Max3PCBatchWait = {}'.format(tconf.Max3PCBatchWait))

    assert len(inst_req_streams) > 1

    inst_tms = []
    max_end_ts = 0
    for req_stream in inst_req_streams:
        tm = ThroughputMeasurement(first_ts=0)
        ts = 0

        for ts, reqs_num in req_stream:
            for req in range(reqs_num):
                tm.add_request(ts)

        if ts > max_end_ts:
            max_end_ts = ts

        inst_tms.append(tm)

    inst_throughput = []
    # Calculate throughput after the latest request ordering plus
    # the window size to take into account all the requests in calculation
    for tm in inst_tms:
        inst_throughput.append(
            tm.get_throughput(max_end_ts + tconf.ThroughputInnerWindowSize))

    master_throughput = inst_throughput[0]
    backups_throughput = inst_throughput[1:]
    avg_backup_throughput = sum(backups_throughput) / len(backups_throughput)

    assert (master_throughput / avg_backup_throughput < tconf.DELTA) == \
        expected_is_master_degraded
