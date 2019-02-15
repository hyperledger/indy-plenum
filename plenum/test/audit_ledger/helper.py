from common.serializers.json_serializer import JsonSerializer
from ledger.ledger import Ledger
from plenum.common.constants import CURRENT_PROTOCOL_VERSION


def check_audit_ledger_updated(audit_size_initial, nodes, audit_txns_added):
    audit_size_after = [node.auditLedger.size for node in nodes]
    for i in range(len(nodes)):
        assert audit_size_after[i] == audit_size_initial[i] + audit_txns_added, \
            "{} != {}".format(audit_size_after[i], audit_size_initial[i] + audit_txns_added)
    assert all(audit_size_after[i] == audit_size_initial[i] + audit_txns_added for i in range(len(nodes)))


def check_audit_txn(txn,
                    view_no, pp_seq_no,
                    seq_no, txn_time,
                    ledger_id, txn_root, state_root,
                    pool_size, domain_size, config_size,
                    last_domain_seqno, last_pool_seqno, last_config_seqno):
    expectedLedgerRoots = {}
    if last_domain_seqno:
        expectedLedgerRoots["1"] = last_domain_seqno
    if last_pool_seqno:
        expectedLedgerRoots["0"] = last_pool_seqno
    if last_config_seqno:
        expectedLedgerRoots["2"] = last_config_seqno
    expectedLedgerRoots[str(ledger_id)] = Ledger.hashToStr(txn_root)

    expected = {
        "reqSignature": {},
        "txn": {
            "data": {
                "ledgerRoot": expectedLedgerRoots,
                "ver": "1",
                "viewNo": view_no,
                "ppSeqNo": pp_seq_no,
                "ledgerSize": {
                    "0": pool_size,
                    "1": domain_size,
                    "2": config_size
                },
                "stateRoot": {
                    str(ledger_id): Ledger.hashToStr(state_root),
                }

            },
            "metadata": {
            },
            "protocolVersion": CURRENT_PROTOCOL_VERSION,
            "type": "2",  # AUDIT
        },
        "txnMetadata": {
            "seqNo": seq_no,
            "txnTime": txn_time
        },

        "ver": "1"
    }
    txn = JsonSerializer().serialize(txn)
    expected = JsonSerializer().serialize(expected)
    print(txn)
    print(expected)
    assert expected == txn
