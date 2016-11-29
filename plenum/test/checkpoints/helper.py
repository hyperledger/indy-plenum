def chkChkpoints(nodes, total: int, stableIndex: int=None):
    for node in nodes:
        for r in node.replicas:
            assert len(r.checkpoints) == total
            if stableIndex is not None:
                assert r.checkpoints.values()[stableIndex].isStable
            else:
                for state in r.checkpoints.values():
                    assert not state.isStable
