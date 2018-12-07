import json

from plenum.common.prepare_batch import split_messages_on_batches, SPLIT_STEPS_LIMIT
from plenum.common.util import randomString

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



def test_small_msgs_with_one_huge_more_than_one_batch():
    msgs = [b'1', b'1', b'1', b'1' * MAX_ONE_MSG_LEN, b'1']
    assert len(split_ut(msgs)) == 3


def test_one_msg_excesses_limit_split_fails():
    msgs = [b'1' * (LEN_LIMIT_BYTES + 1)]
    assert split_ut(msgs) is None


def test_one_msg_almost_excesses_limit_split_fails():
    msgs = [b'1' * (MAX_ONE_MSG_LEN + 1)]
    assert split_ut(msgs) is None


def test_split_messages_on_batches():
    str_len = 10
    max_depth = 2 ** 9
    msg_limit = len(json.dumps([{1: randomString(str_len)}]))
    msgs = [{1: randomString(str_len)} for i in range(max_depth)]
    res = split_messages_on_batches(msgs, json.dumps, lambda l: l <= msg_limit)
    assert res is not None


def test_split_messages_by_size():
    str_len = 10
    msg_count = 100
    msg_limit = len(json.dumps([{1: randomString(str_len)}]))
    msgs = [{1: randomString(str_len)} for i in range(msg_count)]
    res = split_messages_on_batches(msgs, json.dumps, lambda l: l <= msg_limit)
    assert len(res) == msg_count


def test_no_split_if_msg_size_less_then_limit():
    msg_limit = 100
    msgs = [{1: randomString(10)}]
    res = split_messages_on_batches(msgs, json.dumps, lambda l: l <= msg_limit)
    assert len(res) == 1


def test_no_batch_if_msg_size_more_then_limit():
    msg_limit = 100
    msgs = [{1: randomString(101)}]
    res = split_messages_on_batches(msgs, json.dumps, lambda l: l <= msg_limit)
    assert res is None


def test_batch_size_limitations():
    msg_limit = 100
    msgs = [{1: randomString(10)}] * 100
    res = split_messages_on_batches(msgs, json.dumps, lambda l: l <= msg_limit)
    for r in res:
        batch, length = r
        assert len(batch) <= msg_limit
