from plenum.server.suspicion_codes import Suspicions
from plenum.test.helper import getNodeSuspicions


def get_timestamp_suspicion_count(node):
    return len(getNodeSuspicions(node, Suspicions.PPR_TIME_WRONG.code))
