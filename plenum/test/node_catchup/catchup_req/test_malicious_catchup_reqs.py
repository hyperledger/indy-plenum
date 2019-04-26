from plenum.common.messages.node_messages import CatchupReq


def test_catchup_req_for_seq_no_zero(txnPoolNodeSet):
    node = txnPoolNodeSet[0]
    seeder = node.ledgerManager._node_seeder

    req = CatchupReq(ledgerId=1,
                     seqNoStart=0,
                     seqNoEnd=0,
                     catchupTill=1)
    # This shouldn't crash
    seeder.process_catchup_req(req, 'Node2')
