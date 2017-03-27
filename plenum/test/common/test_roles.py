from plenum.common.constants import STEWARD, TRUSTEE
from plenum.common.roles import Roles


def testRolesAreEncoded():
    assert STEWARD == "2"
    assert TRUSTEE == "0"


def testRolesEnumDecoded():
    assert Roles.STEWARD.name == "STEWARD"
    assert Roles.TRUSTEE.name == "TRUSTEE"


def testRolesEnumEncoded():
    assert Roles.STEWARD.value == "2"
    assert Roles.TRUSTEE.value == "0"
