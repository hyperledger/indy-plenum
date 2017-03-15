from plenum.common.roles import Roles
from plenum.common.txn import STEWARD, TRUSTEE, TGB, TRUST_ANCHOR


def testRolesAreEncoded():
    assert STEWARD == "2"
    assert TRUSTEE == "0"
    assert TGB == "1"
    assert TRUST_ANCHOR == "3"


def testRolesEnumDecoded():
    assert Roles.STEWARD.name == "STEWARD"
    assert Roles.TRUSTEE.name == "TRUSTEE"
    assert Roles.TGB.name == "TGB"
    assert Roles.TRUST_ANCHOR.name == "TRUST_ANCHOR"


def testRolesEnumEncoded():
    assert Roles.STEWARD.value == "2"
    assert Roles.TRUSTEE.value == "0"
    assert Roles.TGB.value == "1"
    assert Roles.TRUST_ANCHOR.value == "3"
