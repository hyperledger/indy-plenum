import pytest

from plenum.server.quorums import Quorums
from plenum.server.view_change.node_view_changer import create_view_changer
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
    view_changer = create_view_changer(node)
    return view_changer
