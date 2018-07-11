import types

from plenum.test.helper import sdk_send_random_and_check
from plenum.test.test_node import TestNode


def test_restart_clientstack_before_reply_on_3_of_4_nodes(looper,
                                                          txnPoolNodeSet,
                                                          sdk_pool_handle,
                                                          sdk_wallet_steward):
    orig_send_reply = TestNode.sendReplyToClient
    def send_after_restart(self, reply, reqKey):
        self.restart_clientstack()
        orig_send_reply(self, reply, reqKey)

    def patch_sendReplyToClient():
        for node in txnPoolNodeSet[:3]:
            node.sendReplyToClient = types.MethodType(send_after_restart,
                                                      node)
    def revert_origin_back():
        for node in txnPoolNodeSet:
            node.sendReplyToClient = types.MethodType(orig_send_reply,
                                                      node)

    patch_sendReplyToClient()
    sdk_send_random_and_check(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_steward,
                              1)
    revert_origin_back()
