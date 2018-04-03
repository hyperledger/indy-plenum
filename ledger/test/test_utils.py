import random

from ledger.util import has_nth_bit_set


def test_nth_bit_set():
    """
    Check if bits of randomly chosen integers are set or unset and compare
    with the binary representation.
    """
    for _ in range(0, 10000):
        number = random.randint(0, 100000000)
        bits = bin(number)[2:]
        for i, b in enumerate(reversed(bits)):
            assert has_nth_bit_set(number, i) == (int(b) == 1)
