import pytest


@pytest.fixture(scope='function')
def tempdir(tmpdir_factory):
    return tmpdir_factory.mktemp('').strpath
