# -*- coding: utf-8 -*-
"""
raet unit test package

To run all the unittests:

from raetpoc import test
test.run()

"""
import os
import sys
import unittest


# TODO
import pytest

# TODO CLEANUP move these comments into stories or get rid of them
"""
After key joining party, connections should be very fast!!! Don't need to rejoin!!!
Join is for exchanging long term keys
Allow is for exchanging short term keys (session keys)
Could use a root private key for bootstrapping (hierarchically?) long-term keys for joining.
Seed can be something like a passphrase. Maybe keypairs should all be generated from seeds?
Joining is risky...
    Man in the middle and impostors
    Could we use the distributed consensus pool to secure the key-sharing???
    Out-of-band negotiation of keys with an Agent?
    Agent's can take their info, verify the phone number and email address
    What about node's public keys rotating periodically, and updating bitcoin's blockchain?
    What if the verification keys for the nodes on the blockchain are published by several agents?
    Agents could help bootstrap "clients", providing a key-exchange service, so that
We can control the key that is used in the initial join.


"""


def run():
    # TODO give options here for doing "smoke" tests only, or a full test run
    pytest.main()

if __name__ == "__main__":
    run()

"""
test package


"""
# TODO CLEANUP This is the boilerplate from Sam. Decide how it fits in now.
# from __future__ import absolute_import, division, print_function
#
# import sys
# import os
# import unittest
#
# from ioflo.aid.sixing import *
# from ioflo import test
#
# from ioflo.base.consoling import getConsole
# console = getConsole()
# console.reinit(verbosity=console.Wordage.concise)
#
# top = os.path.dirname(os.path.dirname
#                       (os.path.abspath
#                        (sys.modules.get(__name__).__file__)))
#
#
# if __name__ == "__main__":
#     test.run(top)
