from typing import NamedTuple
import inspect


Suspicion = NamedTuple("SuspicionCode", [("code", int), ("reason", str)])


class Suspicions:
    PPR_TO_PRIMARY = \
        Suspicion(1, "PRE-PREPARE being sent to primary")
    DUPLICATE_PPR_SENT = \
        Suspicion(2,
                  "PRE-PREPARE being sent twice with the same view no and "
                  "sequence no")
    DUPLICATE_PR_SENT = \
        Suspicion(3, "PREPARE request already received")
    UNKNOWN_PR_SENT = \
        Suspicion(4, "PREPARE request for unknown PRE-PREPARE request")
    PR_DIGEST_WRONG = \
        Suspicion(5, "PREPARE request digest is incorrect")
    UNKNOWN_CM_SENT = \
        Suspicion(6, "Commit requests when no prepares received")
    CM_DIGEST_WRONG = \
        Suspicion(7, "Commit requests has incorrect digest")
    DUPLICATE_CM_SENT = \
        Suspicion(8, "COMMIT message has already received")
    PPR_FRM_NON_PRIMARY = \
        Suspicion(9, "Pre-Prepare received from non primary")
    PR_FRM_PRIMARY = \
        Suspicion(10, "Prepare received from primary")
    PPR_DIGEST_WRONG = \
        Suspicion(11, "Pre-Prepare message has incorrect digest")
    DUPLICATE_INST_CHNG = \
        Suspicion(12, "Duplicate instance change message received")
    FREQUENT_INST_CHNG = \
        Suspicion(13, "Too many instance change messages received")
    DUPLICATE_NOM_SENT = \
        Suspicion(14, "NOMINATION request already received")
    DUPLICATE_PRI_SENT = \
        Suspicion(15, "PRIMARY request already received")
    DUPLICATE_REL_SENT = \
        Suspicion(16, "REELECTION request already received")
    WRONG_PPSEQ_NO = \
        Suspicion(17, "Wrong PRE-PREPARE seq number")
    PR_TIME_WRONG = \
        Suspicion(5, "PREPARE time does not match with PRE-PREPARE")
    CM_TIME_WRONG = \
        Suspicion(5, "COMMIT time does not match with PRE-PREPARE")
    PPR_REJECT_WRONG = \
        Suspicion(16, "Pre-Prepare message has incorrect reject")
    PPR_PRE_STATE_WRONG = \
        Suspicion(17, "Pre-Prepare message has incorrect pre-state trie root")
    PPR_PRE_TXN_WRONG = \
        Suspicion(18, "Pre-Prepare message has incorrect pre-transaction tree root")
    PR_PRE_STATE_WRONG = \
        Suspicion(19, "Prepare message has incorrect pre-state trie root")
    PR_PRE_TXN_WRONG = \
        Suspicion(20, "Prepare message has incorrect pre-transaction tree root")
    PPR_POST_STATE_WRONG = \
        Suspicion(21, "Pre-Prepare message has incorrect post-state trie root")
    PPR_POST_TXN_WRONG = \
        Suspicion(22, "Pre-Prepare message has incorrect post-transaction tree root")
    PR_POST_STATE_WRONG = \
        Suspicion(23, "Prepare message has incorrect post-state trie root")
    PR_POST_TXN_WRONG = \
        Suspicion(24, "Prepare message has incorrect post-transaction tree root")
    PRIMARY_DEGRADED = Suspicion(21, 'Primary of master protocol instance '
                                     'degraded the performance')
    PRIMARY_DISCONNECTED = Suspicion(22, 'Primary of master protocol instance '
                                         'disconnected')
    PRIMARY_ABOUT_TO_BE_DISCONNECTED = Suspicion(23, 'Primary of master '
                                                     'protocol instance '
                                                     'about to be disconnected')

    @classmethod
    def get_list(cls):
        return [member for nm, member in inspect.getmembers(cls) if isinstance(
            member, Suspicion)]

    @classmethod
    def get_by_code(cls, code):
        for s in Suspicions.get_list():
            if code == s.code:
                return s
