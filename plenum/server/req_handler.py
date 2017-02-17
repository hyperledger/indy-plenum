from binascii import unhexlify

from plenum.common.types import f


class ReqHandler:
    @staticmethod
    def commit(state, ledger, txnCount, stateRoot, txnRoot):
        (seqNoStart, seqNoEnd), committedTxns = ledger.commitTxns(txnCount)
        stateRoot = unhexlify(stateRoot.encode())
        txnRoot = ledger.hashToStr(unhexlify(txnRoot.encode()))
        assert ledger.root_hash == txnRoot
        state.commit(rootHash=stateRoot)
        r = []
        for txn, seqNo in zip(committedTxns,
                              range(seqNoStart, seqNoEnd + 1)):
            txn[f.SEQ_NO.nm] = seqNo
            r.append(txn)
        return r
