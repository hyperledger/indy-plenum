import time

from plenum.common.constants import KeyValueStorageType
from plenum.recorder.combined_recorder import CombinedRecorder
from plenum.recorder.recorder import Recorder

from storage.helper import initKeyValueStorageIntKeys

from plenum.test.recorder.helper import create_recorder_for_test


def test_combined_recorder(tmpdir_factory):
    r1 = create_recorder_for_test(tmpdir_factory, 'r1')
    r2 = create_recorder_for_test(tmpdir_factory, 'r2')

    msg1, frm1, t1 = 'm1', 'f1', 1
    msg2, frm2, t2 = 'm2', 'f2', 2
    msg3, frm3 = 'm3', 'f3'
    msg4, frm4, t4 = 'm4', 'f4', 4
    r1.add_incoming(msg1, frm1, t1)
    time.sleep(.2)
    r1.add_incoming(msg2, frm2, t2)
    time.sleep(.1)
    r2.add_outgoing(msg3, frm3)
    time.sleep(.1)
    r1.add_incoming(msg4, frm4, t4)
    time.sleep(.1)
    r2.add_disconnecteds('a', 'b', 'c')
    time.sleep(.1)
    r1.add_disconnecteds('x', 'y')

    kv_store = initKeyValueStorageIntKeys(KeyValueStorageType.Leveldb,
                                          tmpdir_factory.mktemp('').strpath,
                                          'combined_recorder')
    cr = CombinedRecorder(kv_store)

    assert not cr.recorders
    cr.add_recorders(r1, r2)
    assert len(cr.recorders) == 2

    cr.combine_recorders()

    cr.start_playing()
    start = time.perf_counter()

    i = 0
    while cr.is_playing and (time.perf_counter() < start + 10):
        vals = cr.get_next()
        if vals:
            if i == 0:
                # Incoming from r1
                assert vals == [[[Recorder.INCOMING_FLAG, msg1, frm1, t1]], []]
            if i == 1:
                # Incoming from r1
                assert vals == [[[Recorder.INCOMING_FLAG, msg2, frm2, t2]], []]
            if i == 2:
                # Outgoing from r2
                assert vals == [[], [[Recorder.OUTGOING_FLAG, msg3, frm3]]]
            if i == 3:
                # Incoming from r1
                assert vals == [[[Recorder.INCOMING_FLAG, msg4, frm4, t4]], []]
            if i == 4:
                # Disconnected from r2
                assert vals == [[], [[Recorder.DISCONN_FLAG, 'a', 'b', 'c']]]
            if i == 5:
                # Disconnected from r1
                assert vals == [[[Recorder.DISCONN_FLAG, 'x', 'y']], []]
            i += 1
