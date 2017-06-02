import pytest


@pytest.fixture(scope="module")
def disable_view_change_config(tconf):
    tconf.unsafe.add('disable_view_change')
    return tconf


def test_disable_view_change(disable_view_change_config, simulate_slow_master):
    assert disable_view_change_config
    assert isinstance(disable_view_change_config.unsafe, set)
    assert 'disable_view_change' in disable_view_change_config.unsafe

    with pytest.raises(RuntimeError) as e_info:
        simulate_slow_master()
    assert e_info.value.args == ('view did not change',)
