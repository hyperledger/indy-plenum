import os

from plenum.common.stacks import nodeStackClass
from stp_zmq.util import createCertsFromKeys
from stp_zmq.zstack import ZStack

from plenum.common.constants import CLIENT_STACK_SUFFIX


def initLocalKeys(name, baseDir, sigseed, override=False, config=None):
    pubkey, verkey = nodeStackClass.initLocalKeys(name, baseDir, sigseed,
                                            override=override)
    print("Public key is", pubkey)
    print("Verification key is", verkey)
    return pubkey, verkey


def initRemoteKeys(name, baseDir, sigseed, verkey, override=False, config=None):
    nodeStackClass.initRemoteKeys(name, baseDir, sigseed, verkey,
                                            override=override)


def initNodeKeysForBothStacks(name, baseDir, sigseed, override=False, config=None):
    nodeStackClass.initLocalKeys(name, baseDir, sigseed, override=override)
    # Above and below method call will return same thing, as seed is same.
    return nodeStackClass.initLocalKeys(name + CLIENT_STACK_SUFFIX, baseDir, sigseed,
                       override=override)


def areKeysSetup(name, baseDir, config=None):
    return nodeStackClass.areKeysSetup(name, baseDir)


def learnKeysFromOthers(baseDir, nodeName, otherNodes):
    homeDir = ZStack.homeDirPath(baseDir, nodeName)
    verifDirPath = ZStack.verifDirPath(homeDir)
    pubDirPath = ZStack.publicDirPath(homeDir)
    for d in (homeDir, verifDirPath, pubDirPath):
        os.makedirs(d, exist_ok=True)
    for otherNode in otherNodes:
        for stack in (otherNode.nodestack, otherNode.clientstack):
            createCertsFromKeys(verifDirPath, stack.name,
                                stack.verKey)
            createCertsFromKeys(pubDirPath, stack.name,
                                stack.publicKey)


def tellKeysToOthers(node, otherNodes):
    for otherNode in otherNodes:
        for attrName in ('nodestack', 'clientstack'):
            stack = getattr(node, attrName)
            otherStack = getattr(otherNode, attrName)
            createCertsFromKeys(otherStack.verifKeyDir, stack.name,
                                stack.verKey)
            createCertsFromKeys(otherStack.publicKeysDir, stack.name,
                                stack.publicKey)