from plenum.common.util import randomString


def test_random_string():
    '''
    Check that max hex digit "f" can be achieved.
    For the task INDY-1754
    '''
    for i in range(100000):
        if randomString()[-1] == 'f':
            return
    assert False
