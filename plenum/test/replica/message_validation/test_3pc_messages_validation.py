from plenum.common.messages.node_messages import Prepare


@pytest.fixture(scope='function')
def replica(tconf, viewNo, inst_id, request):
    node = ReplicaFakeNode(viewNo=viewNo,
                           quorums=Quorums(getValueFromModule(request, 'nodeCount', default=4)))
    bls_bft_replica = FakeSomething(
        gc=lambda *args: None,
        update_pre_prepare=lambda params, l_id: params
    )
    replica = Replica(
        node, instId=inst_id, isMaster=inst_id == 0,
        config=tconf, bls_bft_replica=bls_bft_replica
    )
    return replica


def test_dispatch_three_phase_msg_with_stash(replica):
    sender = "Node1"
    msg = Prepare(inst_id, view_no, pp_seq_no)
    replica.dispatchThreePhaseMsg(msg, sender)