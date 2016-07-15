from binascii import unhexlify

import base64
import json
import os
from collections import OrderedDict

from raet.nacling import Signer, Privateer
from raet.road.keeping import RoadKeep

from plenum.common.util import hasKeys


def initLocalKeep(name, baseDir, pkseed, sigseed, override=False):
    """
    Initialize RAET local keep. Write local role data to file.

    :param name: name of the node
    :param baseDir: base directory
    :param pkseed: seed to generate public and private key pair
    :param sigseed: seed to generate signing and verification key pair
    :param override: overwrite the local role.json file if already exists
    :return: tuple(public key, verification key)
    """
    rolePath = os.path.join(baseDir, name, "role", "local", "role.json")
    if os.path.isfile(rolePath):
        if not override:
            raise FileExistsError("Keys exists for local role {}".format(name))

    if pkseed and not isinstance(pkseed, bytes):
        pkseed = pkseed.encode()
    if sigseed and not isinstance(sigseed, bytes):
        sigseed = sigseed.encode()

    priver = Privateer(pkseed)
    signer = Signer(sigseed)
    keep = RoadKeep(stackname=name, baseroledirpath=baseDir)
    prikey, pubkey = priver.keyhex, priver.pubhex
    sigkey, verkey = signer.keyhex, signer.verhex
    data = OrderedDict([
        ("role", name),
        ("prihex", prikey),
        ("sighex", sigkey)
    ])
    keep.dumpLocalRoleData(data)
    return pubkey.decode(), verkey.decode()


def initRemoteKeep(name, remoteName, baseDir, pubkey, verkey, override=False):
    """
    Initialize RAET remote keep

    :param name: name of the node
    :param remoteName: name of the remote to store keys for
    :param baseDir: base directory
    :param pubkey: public key of the remote
    :param verkey: private key of the remote
    :param override: overwrite the role.remoteName.json file if it already
    exists.
    """
    rolePath = os.path.join(baseDir, name, "role", "remote", "role.{}.json".
                            format(remoteName))
    if os.path.isfile(rolePath):
        if not override:
            raise FileExistsError("Keys exists for remote role {}".
                                  format(remoteName))

    keep = RoadKeep(stackname=name, baseroledirpath=baseDir)
    data = OrderedDict([
        ('role', remoteName),
        ('acceptance', 1),
        ('pubhex', pubkey),
        ('verhex', verkey)
    ])
    keep.dumpRemoteRoleData(data, role=remoteName)


def isLocalKeepSetup(name, baseDir=None) -> bool:
    """
    Check that the local RAET keep has the values of role, sighex and prihex
    populated for the given node

    :param name: the name of the node to check the keys for
    :param baseDir: base directory of Plenum
    :return: whether the keys are setup
    """
    localRoleData = getLocalKeep(name=name, baseDir=baseDir)
    return hasKeys(localRoleData, ['role', 'sighex', 'prihex'])


def getLocalKeep(name, baseDir=None):
    keep = RoadKeep(stackname=name, baseroledirpath=baseDir)
    localRoleData = keep.loadLocalRoleData()
    return localRoleData


def getLocalRoleKeyByName(roleName, baseDir, keyName):
    localRoleData = getLocalKeep(roleName, baseDir)
    keyhex = localRoleData.get(keyName)
    keyhex = str(keyhex) if keyhex is not None else None
    if keyhex is None:
        raise BaseException("Seems {} keypair is not created yet".format(roleName))
    return keyhex


def getLocalVerKey(roleName, baseDir=None):
    sighex = getLocalRoleKeyByName(roleName, baseDir, 'sighex')
    signer = Signer(sighex)
    return signer.verhex.decode()


def getLocalPubKey(roleName, baseDir=None):
    prihex = getLocalRoleKeyByName(roleName, baseDir, 'prihex')
    privateer = Privateer(prihex)
    return privateer.pubhex.decode()


def getEncodedLocalVerKey(name, baseDir=None):
    verKey = getLocalVerKey(name, baseDir)
    return base64.b64encode(unhexlify(verKey)).decode("utf-8")


def getLocalEstateData(name, baseDir):
    estatePath = os.path.expanduser(os.path.join(baseDir, name, "local", "estate.json"))
    if os.path.isfile(estatePath):
        return json.loads(open(estatePath).read())

# def clearLocalKeep(name, keepDir):
#     if not os.path.exists(keepDir):
#         raise FileExistsError("Keep directory does not exist at {}".
#                               format(keepDir))
#     keep = RoadKeep(stackname=name, basedirpath=keepDir, baseroledirpath=keepDir)
#     keep.clearLocalData()
#     keep.clearLocalRoleData()
#
#
# def clearRemoteKeep(name, remoteName, keepDir):
#     if not os.path.exists(keepDir):
#         raise FileExistsError("Keep directory does not exist at {}".
#                               format(keepDir))
#     keep = RoadKeep(stackname=name, basedirpath=keepDir, baseroledirpath=keepDir)
#     keep.clearRemoteData(remoteName)
#     keep.clearRemoteRoleData(remoteName)
