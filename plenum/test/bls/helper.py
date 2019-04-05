from contextlib import contextmanager

import base58
import os

from crypto.bls.bls_crypto import BlsCryptoVerifier
from plenum.bls.bls_crypto_factory import create_default_bls_crypto_factory
from plenum.common.request import Request
from plenum.common.txn_util import get_type, reqToTxn
from plenum.server.quorums import Quorums
from crypto.bls.bls_multi_signature import MultiSignatureValue
from state.pruning_state import PruningState
from common.serializers.serialization import state_roots_serializer, proof_nodes_serializer
from plenum.common.constants import DOMAIN_LEDGER_ID, STATE_PROOF, MULTI_SIGNATURE, \
    MULTI_SIGNATURE_PARTICIPANTS, MULTI_SIGNATURE_SIGNATURE, MULTI_SIGNATURE_VALUE
from plenum.common.keygen_utils import init_bls_keys
from plenum.common.util import hexToFriendly
from plenum.test.helper import sdk_send_random_and_check, create_commit_bls_sig
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.pool_transactions.helper import sdk_send_update_node, \
    sdk_pool_refresh
from stp_core.common.log import getlogger

logger = getlogger()


@contextmanager
def update_validate_bls_signature_without_key_proof(txnPoolNodeSet, value):
    default_param = {}
    for n in txnPoolNodeSet:
        config = n.bls_bft.bls_key_register._pool_manager.config
        default_param[n.name] = config.VALIDATE_BLS_SIGNATURE_WITHOUT_KEY_PROOF
        config.VALIDATE_BLS_SIGNATURE_WITHOUT_KEY_PROOF = value
    yield value
    for n in txnPoolNodeSet:
        n.bls_bft.bls_key_register._pool_manager. \
            config.VALIDATE_BLS_SIGNATURE_WITHOUT_KEY_PROOF = default_param[n.name]


def generate_state_root():
    return base58.b58encode(os.urandom(32)).decode("utf-8")


def sdk_check_bls_multi_sig_after_send(looper, txnPoolNodeSet,
                                       sdk_pool_handle, sdk_wallet_handle,
                                       saved_multi_sigs_count):
    # at least two because first request could have no
    # signature since state can be clear
    number_of_requests = 3

    # 1. send requests
    # Using loop to avoid 3pc batching
    state_roots = []
    for i in range(number_of_requests):
        sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                  sdk_wallet_handle, 1)
        waitNodeDataEquality(looper, txnPoolNodeSet[0], *txnPoolNodeSet[:-1])
        state_roots.append(
            state_roots_serializer.serialize(
                bytes(txnPoolNodeSet[0].getState(DOMAIN_LEDGER_ID).committedHeadHash)))

    # 2. get all saved multi-sigs
    multi_sigs_for_batch = []
    for state_root in state_roots:
        multi_sigs = []
        for node in txnPoolNodeSet:
            multi_sig = node.bls_bft.bls_store.get(state_root)
            if multi_sig:
                multi_sigs.append(multi_sig)
        multi_sigs_for_batch.append(multi_sigs)

    # 3. check how many multi-sigs are saved
    for multi_sigs in multi_sigs_for_batch:
        assert len(multi_sigs) == saved_multi_sigs_count, \
            "{} != {}".format(len(multi_sigs), saved_multi_sigs_count)

    # 3. check that bls multi-sig is the same for all nodes we get PrePrepare for (that is for all expect the last one)
    for multi_sigs in multi_sigs_for_batch[:-1]:
        if multi_sigs:
            assert multi_sigs.count(multi_sigs[0]) == len(multi_sigs)


def process_commits_for_key(key, pre_prepare, bls_bfts):
    for sender_bls_bft in bls_bfts:
        commit = create_commit_bls_sig(
            sender_bls_bft,
            key,
            pre_prepare,
            frm=sender_bls_bft.node_id
        )
        for verifier_bls_bft in bls_bfts:
            verifier_bls_bft.process_commit(commit)


def process_ordered(key, bls_bfts, pre_prepare, quorums):
    for bls_bft in bls_bfts:
        bls_bft.process_order(key,
                              quorums,
                              pre_prepare)


def calculate_multi_sig(creator, bls_bft_with_commits, quorums, pre_prepare):
    key = (0, 0)
    for bls_bft_with_commit in bls_bft_with_commits:
        commit = create_commit_bls_sig(
            bls_bft_with_commit,
            key,
            pre_prepare,
            frm=bls_bft_with_commit.node_id
        )
        creator.process_commit(commit)

    if not creator._can_calculate_multi_sig(key, quorums):
        return None

    return creator._calculate_multi_sig(key, pre_prepare)


def sdk_change_bls_key(looper, txnPoolNodeSet,
                       node,
                       sdk_pool_handle,
                       sdk_wallet_steward,
                       add_wrong=False,
                       new_bls=None,
                       new_key_proof=None,
                       check_functional=True):
    if add_wrong:
        _, new_blspk, key_proof = create_default_bls_crypto_factory().generate_bls_keys()
    else:
        new_blspk, key_proof = init_bls_keys(node.keys_dir, node.name)
    key_in_txn = new_bls or new_blspk
    bls_key_proof = new_key_proof or key_proof
    node_dest = hexToFriendly(node.nodestack.verhex)
    sdk_send_update_node(looper, sdk_wallet_steward,
                         sdk_pool_handle,
                         node_dest, node.name,
                         None, None,
                         None, None,
                         bls_key=key_in_txn,
                         services=None,
                         key_proof=bls_key_proof)
    poolSetExceptOne = list(txnPoolNodeSet)
    poolSetExceptOne.remove(node)
    waitNodeDataEquality(looper, node, *poolSetExceptOne)
    sdk_pool_refresh(looper, sdk_pool_handle)
    if check_functional:
        sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_steward, sdk_pool_handle)
    return new_blspk


def check_bls_key(blskey, node, nodes, add_wrong=False):
    '''
    Check that each node has the same and correct blskey for this node
    '''
    keys = set()
    for n in nodes:
        keys.add(n.bls_bft.bls_key_register.get_key_by_name(node.name))
    assert len(keys) == 1
    if not add_wrong:
        assert blskey == next(iter(keys))

    # check that this node has correct blskey
    if not add_wrong:
        assert node.bls_bft.can_sign_bls()
        assert blskey == node.bls_bft.bls_crypto_signer.pk
    else:
        assert not node.bls_bft.can_sign_bls()


def check_update_bls_key(node_num, saved_multi_sigs_count,
                         looper, txnPoolNodeSet,
                         sdk_wallet_stewards,
                         sdk_wallet_client,
                         sdk_pool_handle,
                         add_wrong=False):
    # 1. Change BLS key for a specified NODE
    node = txnPoolNodeSet[node_num]
    sdk_wallet_steward = sdk_wallet_stewards[node_num]
    new_blspk = sdk_change_bls_key(looper, txnPoolNodeSet,
                                   node,
                                   sdk_pool_handle,
                                   sdk_wallet_steward,
                                   add_wrong)

    # 2. Check that all Nodes see the new BLS key value
    check_bls_key(new_blspk, node, txnPoolNodeSet, add_wrong)

    # 3. Check that we can send new requests and have correct multisigs
    sdk_check_bls_multi_sig_after_send(looper, txnPoolNodeSet,
                                       sdk_pool_handle, sdk_wallet_client,
                                       saved_multi_sigs_count)


def validate_proof_for_read(result, req):
    """
    Validates state proof
    """
    state_root_hash = result[STATE_PROOF]['root_hash']
    state_root_hash = state_roots_serializer.deserialize(state_root_hash)
    proof_nodes = result[STATE_PROOF]['proof_nodes']
    if isinstance(proof_nodes, str):
        proof_nodes = proof_nodes.encode()
    proof_nodes = proof_nodes_serializer.deserialize(proof_nodes)
    key, value = prepare_for_state_read(req)
    valid = PruningState.verify_state_proof(state_root_hash,
                                            key,
                                            value,
                                            proof_nodes,
                                            serialized=True)
    return valid


def validate_proof_for_write(result):
    """
    Validates state proof
    """
    state_root_hash = result[STATE_PROOF]['root_hash']
    state_root_hash = state_roots_serializer.deserialize(state_root_hash)
    proof_nodes = result[STATE_PROOF]['proof_nodes']
    if isinstance(proof_nodes, str):
        proof_nodes = proof_nodes.encode()
    proof_nodes = proof_nodes_serializer.deserialize(proof_nodes)
    key, value = prepare_for_state(result)
    valid = PruningState.verify_state_proof(state_root_hash,
                                            key,
                                            value,
                                            proof_nodes,
                                            serialized=True)
    return valid


def prepare_for_state(result):
    if get_type(result) == "buy":
        from plenum.test.test_node import TestDomainRequestHandler
        key, value = TestDomainRequestHandler.prepare_buy_for_state(result)
        return key, value


def prepare_for_state_read(req: Request):
    if req.txn_type == "buy":
        from plenum.test.test_node import TestDomainRequestHandler
        txn = reqToTxn(req)
        key, value = TestDomainRequestHandler.prepare_buy_for_state(txn)
        return key, value


def validate_multi_signature(state_proof, txnPoolNodeSet):
    """
    Validates multi signature
    """
    multi_signature = state_proof[MULTI_SIGNATURE]
    if not multi_signature:
        logger.debug("There is a state proof, but no multi signature")
        return False

    participants = multi_signature[MULTI_SIGNATURE_PARTICIPANTS]
    signature = multi_signature[MULTI_SIGNATURE_SIGNATURE]
    value = MultiSignatureValue(
        **(multi_signature[MULTI_SIGNATURE_VALUE])
    ).as_single_value()
    quorums = Quorums(len(txnPoolNodeSet))
    if not quorums.bls_signatures.is_reached(len(participants)):
        logger.debug("There is not enough participants of "
                     "multi-signature")
        return False
    public_keys = []
    for node_name in participants:
        key = next(node.bls_bft.bls_crypto_signer.pk for node
                   in txnPoolNodeSet if node.name == node_name)
        if key is None:
            logger.debug("There is no bls key for node {}"
                         .format(node_name))
            return False
        public_keys.append(key)
    _multi_sig_verifier = _create_multi_sig_verifier()
    return _multi_sig_verifier.verify_multi_sig(signature,
                                                value,
                                                public_keys)


def update_bls_keys_no_proof(node_index, sdk_wallet_stewards, sdk_pool_handle, looper, txnPoolNodeSet):
    node = txnPoolNodeSet[node_index]
    sdk_wallet_steward = sdk_wallet_stewards[node_index]
    new_blspk, key_proof = init_bls_keys(node.keys_dir, node.name)
    node_dest = hexToFriendly(node.nodestack.verhex)
    sdk_send_update_node(looper, sdk_wallet_steward,
                         sdk_pool_handle,
                         node_dest, node.name,
                         None, None,
                         None, None,
                         bls_key=new_blspk,
                         services=None,
                         key_proof=None)
    poolSetExceptOne = list(txnPoolNodeSet)
    poolSetExceptOne.remove(node)
    waitNodeDataEquality(looper, node, *poolSetExceptOne)
    sdk_pool_refresh(looper, sdk_pool_handle)
    return new_blspk


def _create_multi_sig_verifier() -> BlsCryptoVerifier:
    verifier = create_default_bls_crypto_factory() \
        .create_bls_crypto_verifier()
    return verifier
