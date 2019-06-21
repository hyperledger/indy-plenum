import pytest

from plenum.common.messages.node_messages import ViewChange, ViewChangeAck
from plenum.server.consensus.view_change_service import ViewChangeVotesForView, ViewChangeService
from plenum.server.quorums import Quorums
from plenum.test.consensus.helper import view_change_message


@pytest.fixture
def quorums(validators):
    return Quorums(len(validators))


@pytest.fixture
def votes(quorums):
    return ViewChangeVotesForView(quorums)


def test_view_change_votes_is_empty_initially(votes):
    assert not votes.has_new_view_quorum


@pytest.mark.skip(reason="Not implemented yet")
def test_n_minus_f_different_votes_with_acks_is_enough_for_new_view_quorum(random, validators, quorums, votes):
    vc_msg_count = random.integer(quorums.view_change.value, len(validators))
    view_change_messages = [(view_change_message(random, view_no=1), frm)
                            for frm in random.sample(validators, vc_msg_count)]

    view_change_acks = []
    for vc, name in view_change_messages:
        not_sender = set(validators) - {name}
        vc_ack_count = random.integer(quorums.view_change_ack.value, len(not_sender))
        view_change_acks.extend(
            (ViewChangeAck(viewNo=1, name=name, digest=ViewChangeService._view_change_digest(vc)), frm)
            for frm in random.sample(not_sender, vc_ack_count))

    messages = random.shuffle(view_change_messages + view_change_acks)
    for msg, frm in messages:
        if isinstance(msg, ViewChange):
            votes.add_view_change(msg, frm)
        elif isinstance(msg, ViewChangeAck):
            votes.add_view_change_ack(msg, frm)

    assert votes.has_new_view_quorum


def test_add_one_view_change_doesnt_affect_quorum(random, some_item, validators, votes):
    vc = view_change_message(random)
    frm = some_item(validators)

    votes.add_view_change(vc, frm)
    assert not votes.has_new_view_quorum


