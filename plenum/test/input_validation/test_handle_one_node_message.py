import pytest

"""
Lets check the handleOneNodeMsg contract
"""


def test_empty_args_fail(testNode):
    before_msg = len(testNode.nodeInBox)
    while pytest.raises(AssertionError):
        testNode.handleOneNodeMsg(())
    assert before_msg == len(testNode.nodeInBox), \
        'nodeInBox has not got a message'


def test_too_many_args_fail(testNode):
    before_msg = len(testNode.nodeInBox)
    testNode.handleOneNodeMsg(({}, 'otherNone', 'extra_arg'))
    while pytest.raises(AssertionError):
        testNode.handleOneNodeMsg(())
    assert before_msg == len(testNode.nodeInBox), \
        'nodeInBox has not got a message'
