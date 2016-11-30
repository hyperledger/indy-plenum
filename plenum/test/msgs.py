from plenum.common.types import TaggedTuple
from plenum.common.util import randomString


def randomMsg() -> TaggedTuple:
    return TestMsg('subject ' + randomString(),
                   'content ' + randomString())


TESTMSG = "TESTMSG"
TestMsg = TaggedTuple(TESTMSG, [
    ("subject", str),
    ("content", str)])