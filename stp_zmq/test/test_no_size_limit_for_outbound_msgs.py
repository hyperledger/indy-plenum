import pytest
import json

from stp_core.loop.eventually import eventually
from stp_core.network.port_dispenser import genHa
from stp_core.test.helper import CounterMsgsHandler, prepStacks
from stp_zmq.test.helper import genKeys
from stp_zmq.zstack import ZStack


MSG_LEN_LIMIT = 250


@pytest.fixture(params=[-1, 0, 1, 1000])
def msg(request):
    """
    We need to generate a message that has a length less/equal/greater than the MSG_LEN_LIMIT.

    If we have a message {'key', ''}, when delivered this message will have the  following format: "{'key', ''}"

    MSG_LEN_LIMIT - len("{'k', ''}") will tell us how many chars the value we place in the message must have in order
    for us to have a message with the same size as MSG_LEN_LIMIT.

    :param request: how many chars less/more will the resulting message have compared to MSG_LEN_LIMIT
    :return:
    """
    offset = request.param

    serialized_msg_no_value = "{'k', ''}"
    value_len = MSG_LEN_LIMIT - len(serialized_msg_no_value)

    value = 'v' * (value_len + offset)
    msg = {'k': value}

    assert len(json.dumps(msg)) == MSG_LEN_LIMIT + offset

    return msg


def test_no_size_limit_for_outbound_msgs(looper, tdir, msg):

    names = ['Alpha', 'Beta']
    genKeys(tdir, names)

    alpha = ZStack(names[0], ha=genHa(), basedirpath=tdir, msgHandler=None, restricted=False)
    alpha.msgLenVal.max_allowed = MSG_LEN_LIMIT

    beta_msg_handler = CounterMsgsHandler()
    beta = ZStack(names[1], ha=genHa(), basedirpath=tdir, msgHandler=beta_msg_handler.handler, restricted=False)

    prepStacks(looper, alpha, beta)

    def check_received(value):
        assert beta_msg_handler.receivedMsgCount == value

    alpha.send(msg)
    looper.run(eventually(check_received, 1, retryWait=1, timeout=10))
