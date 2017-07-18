import pytest


@pytest.fixture()
def viewNo(nodeSet):
    viewNos = set()
    for n in nodeSet:
        viewNos.add(n.viewNo)
    assert len(viewNos) == 1
    return viewNos.pop()


