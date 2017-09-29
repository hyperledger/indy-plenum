import pytest


@pytest.fixture()
def tempdir(tmpdir_factory):
    return tmpdir_factory.mktemp('').strpath
