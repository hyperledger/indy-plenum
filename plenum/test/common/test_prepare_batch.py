from plenum.common.prepare_batch import split_messages_on_batches, SPLIT_STEPS_LIMIT

LEN_LIMIT_BYTES = 100
SERIALIZATION_OTHER_HEAD_BYTES = 10
MAX_ONE_MSG_LEN = LEN_LIMIT_BYTES - SERIALIZATION_OTHER_HEAD_BYTES


def make_batch_func(msgs):
    overhead = b'1' * SERIALIZATION_OTHER_HEAD_BYTES
    return b''.join(msgs + [overhead])


def check_batch_len_func(length):
    return length <= LEN_LIMIT_BYTES


def split_ut(msgs):
    return split_messages_on_batches(msgs, make_batch_func, check_batch_len_func)


def test_empty_msgs_returns_one_batch():
    assert len(split_ut([])) == 1


def test_less_than_limit_returns_one_batch():
    msgs = [b'1'] * 10
    assert len(split_ut(msgs)) == 1


def test_total_len_excesses_limit_two_batches():
    msgs = [b'1'] * (LEN_LIMIT_BYTES + 1)
    assert len(split_ut(msgs)) == 2


def test_each_msg_almost_excesses_limit_one_msg_per_batch():
    count = 100
    msgs = [b'1' * MAX_ONE_MSG_LEN] * count
    assert len(split_ut(msgs)) == count


def test_small_msgs_with_one_huge_more_than_one_batch():
    msgs = [b'1', b'1', b'1', b'1' * MAX_ONE_MSG_LEN, b'1']
    assert len(split_ut(msgs)) == 4


def test_one_msg_excesses_limit_split_fails():
    msgs = [b'1' * (LEN_LIMIT_BYTES + 1)]
    assert split_ut(msgs) is None


def test_one_msg_almost_excesses_limit_split_fails():
    msgs = [b'1' * (MAX_ONE_MSG_LEN + 1)]
    assert split_ut(msgs) is None


def test_excesses_limit_of_split_steps_split_fails():
    msgs = [b'1' * MAX_ONE_MSG_LEN] * 2 ** (SPLIT_STEPS_LIMIT + 1)
    assert split_ut(msgs) is None
