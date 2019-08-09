from plenum.test.helper import assertEquality


def chkChkpoints(nodes, total: int, stableIndex: int = None):
    for i in nodes[0].replicas.keys():
        chk_chkpoints_for_instance(nodes, i, total, stableIndex)


def chk_chkpoints_for_instance(nodes, inst_id, total: int, stableIndex: int = None):
    for node in nodes:
        r = node.replicas.values()[inst_id]
        assert len(r._checkpointer._checkpoint_state) == total, '{} checkpoints {}, whereas total {}'. \
            format(r, len(r._checkpointer._checkpoint_state), total)
        if stableIndex is not None:
            assert r._checkpointer._checkpoint_state.values()[stableIndex].isStable, r.name
        else:
            for state in r._checkpointer._checkpoint_state.values():
                assert not state.isStable


def checkRequestCounts(nodes, req_count, batches_count):
    for node in nodes:
        assertEquality(len(node.requests), req_count)
        for r in node.replicas.values():
            assertEquality(len(r.commits), batches_count)
            assertEquality(len(r.prepares), batches_count)
            assertEquality(len(r.batches), batches_count)


def check_stashed_chekpoints(node, count):
    c = sum(len(ckps)
            for ckps_for_view in node.master_replica._checkpointer._stashed_recvd_checkpoints.values()
            for ckps in ckps_for_view.values())
    assert count == c, "{} != {}".format(count, c)
