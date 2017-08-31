import pytest
import json
from plenum.common.prepare_batch import split_msgs_on_batches


LEN_LIMIT_BYTES = 100
SERIALIZATION_OTHER_HEAD_BYTES = 10


def make_batch_func(msgs):
    overhead = b'1' * SERIALIZATION_OTHER_HEAD_BYTES
    return b''.join(msgs + [overhead])


def check_batch_len_func(length):
    return length <= LEN_LIMIT_BYTES


def split_ut(msgs):
    return split_msgs_on_batches(msgs, make_batch_func, check_batch_len_func)


def test_empty_msgs_returns_one_batch():
    assert len(split_ut([])) == 1


def test_less_than_limit_returns_one_batch():
    msgs = [b'1'] * 10
    assert check_batch_len_func(len(make_batch_func(msgs)))
    assert len(split_ut(msgs)) == 1


def test_msgs_total_len_excesses_limit_more_than_one_batch():
    msgs = [b'1'] * (LEN_LIMIT_BYTES + 1)
    assert not check_batch_len_func(len(make_batch_func(msgs)))
    assert len(split_ut(msgs)) > 1


def test_one_msg_excesses_limit():
    msgs = [b'1' * (LEN_LIMIT_BYTES + 1)]
    assert split_ut(msgs) is None


def test_one_msg_almost_excesses_limit():
    msgs = [b'1' * (LEN_LIMIT_BYTES - SERIALIZATION_OTHER_HEAD_BYTES + 1)]
    assert split_ut(msgs) is None


def test_each_msg_almost_excesses_limit():
    count = 100
    msgs = [b'1' * (LEN_LIMIT_BYTES - SERIALIZATION_OTHER_HEAD_BYTES)] * count
    assert len(split_ut(msgs)) == count


def test_recursion_depth():
    assert split_msgs_on_batches([], make_batch_func, lambda l: False) is None
