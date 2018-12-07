import pytest

from plenum.server.quorums import Quorums
from plenum.server.view_change.view_changer import ViewChanger
from plenum.test.testing_utils import FakeSomething


@pytest.fixture(scope='module')
def view_changer():
    config = FakeSomething(
        ViewChangeWindowSize=1,
        ForceViewChangeFreq=0
    )
    node = FakeSomething(
        name="fake node",
        ledger_ids=[0],
        config=config,
        quorums=Quorums(7)
    )
    view_changer = ViewChanger(node)
    return view_changer
