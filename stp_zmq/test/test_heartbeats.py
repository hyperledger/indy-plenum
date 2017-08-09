import pytest

from stp_zmq.test.helper import create_and_prep_stacks, \
    check_stacks_communicating, add_counters_to_ping_pong


def sent_ping_counts(*stacks):
    return {s.name: s.sent_ping_count for s in stacks}


def sent_pong_counts(*stacks):
    return {s.name: s.sent_pong_count for s in stacks}


def recv_ping_counts(*stacks):
    return {s.name: s.recv_ping_count for s in stacks}


def recv_pong_counts(*stacks):
    return {s.name: s.recv_pong_count for s in stacks}


heartbeat_freq = 2


@pytest.fixture()
def setup(tdir, looper, tconf):
    names = ['Alpha', 'Beta', 'Gamma']
    (alpha, beta, gamma), (alphaP, betaP, gammaP) = \
        create_and_prep_stacks(names, tdir, looper, tconf)
    check_stacks_communicating(looper, (alpha, beta, gamma),
                               (alphaP, betaP, gammaP))
    return (alpha, beta, gamma)


def test_heartbeats_only_one_stack(tdir, looper, tconf, setup):
    """
    Only one of several stacks sends periodic heartbeat messages, other stacks
    acknowledge it but do not send heartbeats
    """
    (alpha, beta, gamma) = setup

    # Only alpha should send heartbeats
    alpha.config.ENABLE_HEARTBEATS = True
    alpha.config.HEARTBEAT_FREQ = heartbeat_freq

    for s in (alpha, beta, gamma):
        add_counters_to_ping_pong(s)

    sent_pings_before = sent_ping_counts(alpha, beta, gamma)
    sent_pongs_before = sent_pong_counts(alpha, beta, gamma)
    recv_pings_before = recv_ping_counts(alpha, beta, gamma)
    recv_pongs_before = recv_pong_counts(alpha, beta, gamma)

    looper.runFor(6 * heartbeat_freq)

    # Only alpha should send pings
    assert sent_ping_counts(alpha)[alpha.name] - \
        sent_pings_before[alpha.name] >= 5
    assert sent_ping_counts(beta)[beta.name] == sent_pings_before[beta.name]
    assert sent_ping_counts(gamma)[gamma.name] == sent_pings_before[gamma.name]

    # All except alpha should receive pings
    assert recv_ping_counts(alpha)[alpha.name] == recv_pings_before[alpha.name]
    assert recv_ping_counts(beta)[beta.name] - \
        recv_pings_before[beta.name] >= 5
    assert recv_ping_counts(gamma)[gamma.name] - \
        recv_pings_before[gamma.name] >= 5

    # All except alpha should send pongs
    assert sent_pong_counts(alpha)[alpha.name] == sent_pongs_before[alpha.name]
    assert sent_pong_counts(beta)[beta.name] - \
        sent_pongs_before[beta.name] >= 5
    assert sent_pong_counts(gamma)[gamma.name] - sent_pongs_before[
        gamma.name] >= 5

    # Only alpha should receive pongs
    assert recv_pong_counts(alpha)[alpha.name] - recv_pongs_before[
        alpha.name] >= 5
    assert recv_pong_counts(beta)[beta.name] == recv_pongs_before[beta.name]
    assert recv_pong_counts(gamma)[gamma.name] == recv_pongs_before[gamma.name]


def test_heartbeats_all_stacks(tdir, looper, tconf, setup):
    """
    All stacks send periodic heartbeat messages and other stacks
    acknowledge it
    """

    (alpha, beta, gamma) = setup

    # All stacks should send heartbeats
    for stack in (alpha, beta, gamma):
        stack.config.ENABLE_HEARTBEATS = True
        stack.config.HEARTBEAT_FREQ = heartbeat_freq

    for s in (alpha, beta, gamma):
        add_counters_to_ping_pong(s)

    sent_pings_before = sent_ping_counts(alpha, beta, gamma)
    sent_pongs_before = sent_pong_counts(alpha, beta, gamma)
    recv_pings_before = recv_ping_counts(alpha, beta, gamma)
    recv_pongs_before = recv_pong_counts(alpha, beta, gamma)

    looper.runFor(6 * heartbeat_freq)

    for stack in (alpha, beta, gamma):
        # All should send pings
        assert sent_ping_counts(
            stack)[stack.name] - sent_pings_before[stack.name] >= 5
        # All should receive pings
        assert recv_ping_counts(
            stack)[stack.name] - recv_pings_before[stack.name] >= 5
        # All except alpha should send pongs
        assert sent_pong_counts(
            stack)[stack.name] - sent_pongs_before[stack.name] >= 5
        # All should receive pongs
        assert recv_pong_counts(
            stack)[stack.name] - recv_pongs_before[stack.name] >= 5
