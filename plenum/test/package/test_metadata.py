import pytest

from plenum.__metadata__ import (
    check_version, set_version, load_version, pep440_version,
    parse_version, __version__
)
from plenum.common.version import PlenumVersion, InvalidVersionError

# TODO ??? other type of cases
def test_check_version_fail():
    for version in [
        '1',
        1,
        (1, 2, 3),
        (1, 2, 3, 4, 5),
        (1, 2, 3, 'alpha', 5),
        (1, 2, 3, 'dev', 5, 6)
    ]:
        with pytest.raises(ValueError):
            check_version(version)


# TODO ??? other type of cases
def test_check_version_pass():
    for version in [
            (1, 2, 3, 'dev', 5),
            (1, 2, 3, 'rc', 5),
            (1, 2, 3, 'stable', 5)
    ]:
        check_version(version)


# TODO ??? other type of cases
def test_set_and_load_version(tmpdir):
    version_file = str(tmpdir.join("version.txt"))
    for version in [
        (1, 2, 3, 'dev', 5),
        (1, 2, 3, 'rc', 5),
        (1, 2, 3, 'stable', 5)
    ]:
        set_version(version, version_file)
        assert load_version(version_file) == list(version)


# checks that current version is valid
def test_pep440_version_default():
    pep440_version()


def test_package_version():
    assert __version__ == pep440_version().full


def test_pep440_version_stable():
    assert pep440_version((1, 2, 3, 'stable', 2)) == PlenumVersion('1.2.3')


def test_pep440_version_dev():
    assert pep440_version((1, 2, 3, 'dev', 1)) == PlenumVersion('1.2.3.dev1')


def test_pep440_version_rc():
    assert pep440_version((1, 2, 3, 'rc', 2)) == PlenumVersion('1.2.3.rc2')


def test_parse_version():
    assert parse_version("1.2.3").parts == (0, 1, 2, 3, None, None, None)
    assert parse_version("1.2.3rc5").parts == (0, 1, 2, 3, 'rc', 5, None)
    assert parse_version("1.2.3dev6").parts == (0, 1, 2, 3, 'dev', 6, None)
    with pytest.raises(InvalidVersionError):
        parse_version("1.2.3.4.5")
