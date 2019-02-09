from plenum.common.util import SortedDict


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
    expectedLedgerRoots[str(ledger_id)] = txn_root

    expected = {
        "reqSignature": {},
        "txn": {
            "data": {
                "ver": "1",
                "viewNo": view_no,
                "ppSeqNo": pp_seq_no,
                "ledgerSize": {
                    "0": pool_size,
                    "1": domain_size,
                    "2": config_size
                },

                "ledgerRoot": expectedLedgerRoots,

                "stateRoot": {
                    str(ledger_id): state_root,
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
    assert SortedDict(expected) == SortedDict(txn)
