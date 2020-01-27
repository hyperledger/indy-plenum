import pytest


CHK_FREQ = 6


@pytest.fixture(scope="module")
def tconf(tconf):
    old_new_view_timeout = tconf.NEW_VIEW_TIMEOUT
    old_batch_size = tconf.Max3PCBatchSize
    old_chk_freq = tconf.CHK_FREQ
    tconf.CHK_FREQ = CHK_FREQ
    tconf.LOG_SIZE = CHK_FREQ * 3
    tconf.NEW_VIEW_TIMEOUT = 5
    tconf.Max3PCBatchSize = 1
    yield tconf
    tconf.Max3PCBatchSize = old_batch_size
    tconf.NEW_VIEW_TIMEOUT = old_new_view_timeout
    tconf.CHK_FREQ = old_chk_freq
