from plenum.test.helper import assertEquality


def chkChkpoints(nodes, total: int, stableIndex: int = None):
    for node in nodes:
        for r in node.replicas:
            assert len(r.checkpoints) == total, '{} checkpoints {}, whereas total {}'. \
                format(r, len(r.checkpoints), total)
            if stableIndex is not None:
                assert r.checkpoints.values()[stableIndex].isStable, r.name
            else:
                for state in r.checkpoints.values():
                    assert not state.isStable


def checkRequestCounts(nodes, req_count, cons_count, batches_count):
    for node in nodes:
        assertEquality(len(node.requests), req_count)
        for r in node.replicas:
            assertEquality(len(r.commits), cons_count)
            assertEquality(len(r.prepares), cons_count)
            assertEquality(len(r.batches), batches_count)
