import os

from plenum.bls.bls_crypto_factory import create_default_bls_crypto_factory
from plenum.common.constants import CLIENT_STACK_SUFFIX
from plenum.common.stacks import nodeStackClass
from stp_core.crypto.util import randomSeed


def initLocalKeys(name, keys_dir, sigseed, *, use_bls, override=False):
    # * forces usage of names for args on the right hand side
    pubkey, verkey = nodeStackClass.initLocalKeys(name, keys_dir, sigseed, override=override)
    print("Public key is", pubkey)
    print("Verification key is", verkey)
    blspk = init_bls_keys(keys_dir, name, sigseed) if use_bls else None
    return pubkey, verkey, blspk


def initRemoteKeys(name, remote_name, keys_dir, verkey, override=False):
    nodeStackClass.initRemoteKeys(name, remote_name, keys_dir, verkey,
                                  override=override)


def init_bls_keys(keys_dir, node_name, seed=None):
    # TODO: do we need keys based on transport keys?
    bls_keys_dir = os.path.join(keys_dir, node_name)
    bls_factory = create_default_bls_crypto_factory(keys_dir=bls_keys_dir)
    stored_pk = bls_factory.generate_and_store_bls_keys(seed)
    print("BLS Public key is", stored_pk)
    return stored_pk


def initNodeKeysForBothStacks(name, keys_dir, sigseed, *, use_bls=True,
                              override=False):
    # `sigseed` is initialised to keep the seed same for both stacks.
    # Both node and client stacks need to have same keys
    if not sigseed:
        sigseed = sigseed or randomSeed()
        print("Generating keys for random seed", sigseed)
    else:
        print("Generating keys for provided seed", sigseed)

    node_stack_name = name
    client_stack_name = node_stack_name + CLIENT_STACK_SUFFIX
    print("Init local keys for client-stack")
    initLocalKeys(client_stack_name, keys_dir, sigseed, use_bls=False, override=override)
    print("Init local keys for node-stack")
    keys = initLocalKeys(node_stack_name, keys_dir, sigseed, use_bls=use_bls, override=override)
    return keys


def areKeysSetup(name, keys_dir):
    return nodeStackClass.areKeysSetup(name, keys_dir)


def learnKeysFromOthers(keys_dir, nodeName, otherNodes):
    otherNodeStacks = []
    for otherNode in otherNodes:
        if otherNode.name != nodeName:
            otherNodeStacks.append(otherNode.nodestack)
            otherNodeStacks.append(otherNode.clientstack)
    nodeStackClass.learnKeysFromOthers(keys_dir, nodeName, otherNodeStacks)


def tellKeysToOthers(node, otherNodes):
    otherNodeStacks = []
    for otherNode in otherNodes:
        if otherNode != node:
            otherNodeStacks.append(otherNode.nodestack)
            otherNodeStacks.append(otherNode.clientstack)

    node.nodestack.tellKeysToOthers(otherNodeStacks)
    node.clientstack.tellKeysToOthers(otherNodeStacks)
