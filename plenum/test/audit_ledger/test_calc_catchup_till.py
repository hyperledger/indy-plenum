from plenum.server.batch_handlers.three_pc_batch import ThreePcBatch


def test_calc_catchup_till_missing_ledger(txnPoolNodeSet):
    node = txnPoolNodeSet[0]

    # We don't have ledger with ID 42, but put it into audit txn
    txn = {
        'txn': {
            'metadata': {},
            'type': '2',
            'protocolVersion': 2,
            'data': {
                'ppSeqNo': 1,
                'viewNo': 0,
                'stateRoot': {
                    0: 'pool_state_root',
                    1: 'domain_state_root',
                    42: '42_state_root'
                },
                'ledgerSize': {
                    0: 4,
                    1: 12,
                    2: 0,
                    42: 73
                },
                'primaries': ['Alpha', 'Beta'],
                'ver': '1',
                'ledgerRoot': {
                    0: 'pool_txn_root',
                    1: 'domain_txn_root',
                    42: '42_txn_root'
                }
            }
        },
        'ver': '1',
        'reqSignature': {},
        'txnMetadata': {}
    }
    node.auditLedger.append_txns_metadata([txn], 3549872341)
    node.auditLedger.appendTxns([txn])
    node.auditLedger.commitTxns(1)

    catchup_till = node.ledgerManager._node_leecher._calc_catchup_till()
