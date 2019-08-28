import zmq

from plenum.test.helper import assertExp
from stp_core.loop.eventually import eventually


def create_msg(i):
    return {'msg': 'msg{}'.format(i)}


def test_stash_msg_to_unknown(tdir, looper, stacks, alpha_handler):
    alpha, beta = stacks
    pending_client_messages = beta._client_message_provider._pending_client_messages
    msg1 = {'msg': 'msg1'}
    msg2 = {'msg': 'msg2'}

    beta.send(msg1, alpha.listener.IDENTITY)
    assert pending_client_messages[alpha.listener.IDENTITY] == [(0, msg1)]

    alpha.connect(name=beta.name, ha=beta.ha,
                  verKeyRaw=beta.verKeyRaw, publicKeyRaw=beta.publicKeyRaw)

    looper.runFor(0.25)

    alpha.send(msg2, beta.name)
    looper.run(eventually(
        lambda msg_handler: assertExp(msg_handler.received_messages == [msg1, msg2]),
        alpha_handler))
    assert not pending_client_messages


def test_resending_only_for_known_clients(tdir, looper, stacks, alpha_handler):
    alpha, beta = stacks
    unknown_identity = "unknown_identity"
    pending_client_messages = beta._client_message_provider._pending_client_messages
    msg1 = {'msg': 'msg1'}
    msg2 = {'msg': 'msg2'}

    beta.send(msg1, alpha.listener.IDENTITY)
    beta.send(msg1, unknown_identity)
    assert pending_client_messages[alpha.listener.IDENTITY] == [(0, msg1)]
    assert pending_client_messages[unknown_identity] == [(0, msg1)]
    message_pending_unknown_id = pending_client_messages[unknown_identity]
    beta._client_message_provider._timer.set_time(1)

    alpha.connect(name=beta.name, ha=beta.ha,
                  verKeyRaw=beta.verKeyRaw, publicKeyRaw=beta.publicKeyRaw)

    looper.runFor(0.25)

    alpha.send(msg2, beta.name)
    looper.run(eventually(
        lambda msg_handler: assertExp(msg_handler.received_messages == [msg1, msg2]),
        alpha_handler))
    assert alpha.listener.IDENTITY not in pending_client_messages
    assert pending_client_messages[unknown_identity] == message_pending_unknown_id


def test_invalid_msgs_are_not_stashed(tdir, looper, stacks, alpha_handler, tconf):
    alpha, beta = stacks
    pending_client_messages = beta._client_message_provider._pending_client_messages
    msg = {'msg': 'msg1' * tconf.MSG_LEN_LIMIT}
    assert not pending_client_messages
    assert not alpha_handler.received_messages

    alpha.connect(name=beta.name, ha=beta.ha,
                  verKeyRaw=beta.verKeyRaw, publicKeyRaw=beta.publicKeyRaw)

    looper.runFor(0.25)

    alpha.send(msg, beta.name)
    assert not pending_client_messages
    assert not alpha_handler.received_messages

    msg = {'msg': 'msg1'}
    alpha.send(msg, beta.name)
    looper.run(eventually(
        lambda msg_handler: assertExp(msg_handler.received_messages == [msg]),
        alpha_handler))


def test_resending_for_old_stash_msgs(tdir, tconf, looper, stacks,
                                      alpha_handler, monkeypatch):
    alpha, beta = stacks
    msg1 = {'msg': 'msg1'}
    pending_client_messages = beta._client_message_provider._pending_client_messages

    alpha.connect(name=beta.name, ha=beta.ha,
                  verKeyRaw=beta.verKeyRaw, publicKeyRaw=beta.publicKeyRaw)
    looper.runFor(0.25)

    def fake_send_multipart(msg_parts, flags=0, copy=True, track=False, **kwargs):
        raise zmq.Again

    monkeypatch.setattr(beta.listener, 'send_multipart',
                        fake_send_multipart)
    alpha.send(msg1, beta.name)
    looper.run(eventually(
        lambda messages: assertExp(messages[alpha.listener.IDENTITY] == [(0, msg1)]),
        pending_client_messages))
    monkeypatch.undo()

    beta._client_message_provider._timer.set_time(tconf.RESEND_CLIENT_MSG_TIMEOUT + 2)
    looper.run(eventually(
        lambda msg_handler: assertExp(msg_handler.received_messages == [msg1]),
        alpha_handler))
    assert not pending_client_messages


def test_limit_msgs_for_client(tconf, looper, stacks, alpha_handler):
    alpha, beta = stacks
    pending_client_messages = beta._client_message_provider._pending_client_messages
    for i in range(tconf.PENDING_MESSAGES_FOR_ONE_CLIENT_LIMIT + 2):
        beta.send(create_msg(i),
                  alpha.listener.IDENTITY)
    assert len(pending_client_messages[alpha.listener.IDENTITY]) == tconf.PENDING_MESSAGES_FOR_ONE_CLIENT_LIMIT
    assert pending_client_messages[alpha.listener.IDENTITY][0] != (0, create_msg(0))
    assert pending_client_messages[alpha.listener.IDENTITY][0] == (0, create_msg(2))
    assert pending_client_messages[alpha.listener.IDENTITY][-1] == (0,
                                                                    create_msg(
                                                                        tconf.PENDING_MESSAGES_FOR_ONE_CLIENT_LIMIT + 1))


def test_limit_pending_queue(tconf, looper, stacks, alpha_handler):
    alpha, beta = stacks
    pending_client_messages = beta._client_message_provider._pending_client_messages
    for i in range(tconf.PENDING_CLIENT_LIMIT + 1):
        beta.send(create_msg(1), str(i))
    assert len(pending_client_messages) == tconf.PENDING_CLIENT_LIMIT
    assert str(0) not in pending_client_messages
    assert str(1) in pending_client_messages
    assert str(tconf.PENDING_CLIENT_LIMIT) in pending_client_messages


def test_removing_old_stash(tdir, looper, tconf, stacks):
    _, node_stack = stacks
    pending_client_messages = node_stack._client_message_provider._pending_client_messages
    msg = create_msg(0)
    client_identity1 = "IDENTITY_1"
    client_identity2 = "IDENTITY_2"
    client_identity3 = "IDENTITY_3"
    node_stack.send(msg, client_identity1)

    # test that old messages have been cleared
    assert pending_client_messages[client_identity1] == [(0, msg)]
    node_stack._client_message_provider._timer.set_time(tconf.REMOVE_CLIENT_MSG_TIMEOUT + 2)
    node_stack.send(msg, client_identity2)
    assert client_identity1 not in pending_client_messages
    assert pending_client_messages[client_identity2] == [(tconf.REMOVE_CLIENT_MSG_TIMEOUT + 2,
                                                          msg)]

    # test that the cleaning will be repeated
    new_time = (tconf.REMOVE_CLIENT_MSG_TIMEOUT + 2) * 2
    node_stack._client_message_provider._timer.set_time(new_time)
    node_stack.send(msg, client_identity3)
    assert client_identity1 not in pending_client_messages
    assert client_identity2 not in pending_client_messages
    assert pending_client_messages[client_identity3] == [(new_time,
                                                          msg)]


def test_correct_removal(looper, stacks, tconf):
    _, node_stack = stacks
    client_identity1 = "IDENTITY1"
    client_identity2 = "IDENTITY2"

    msg_1 = create_msg(0)
    msg_2 = create_msg(1)

    node_stack.send(msg_1, client_identity1)
    node_stack.send(msg_2, client_identity2)

    node_stack._client_message_provider._pending_client_messages['IDENTITY2'][0] = (tconf.REMOVE_CLIENT_MSG_TIMEOUT,
                                                                                    msg_2)

    new_time = tconf.REMOVE_CLIENT_MSG_TIMEOUT
    node_stack._client_message_provider._timer.set_time(new_time)

    node_stack._client_message_provider._remove_old_messages()
