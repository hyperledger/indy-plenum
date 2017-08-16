import json
import os

from stp_core.crypto.nacl_wrappers import Signer, Privateer
from raet.road.keeping import RoadKeep


def getLocalKeep(name, baseDir=None):
    keep = RoadKeep(stackname=name, baseroledirpath=baseDir)
    localRoleData = keep.loadLocalRoleData()
    return localRoleData


def getLocalRoleKeyByName(roleName, baseDir, keyName):
    localRoleData = getLocalKeep(roleName, baseDir)
    keyhex = localRoleData.get(keyName)
    keyhex = str(keyhex) if keyhex is not None else None
    if keyhex is None:
        raise BaseException("Seems {} keypair is not created yet"
                            .format(roleName))
    return keyhex


def getLocalVerKey(roleName, baseDir=None):
    sighex = getLocalRoleKeyByName(roleName, baseDir, 'sighex')
    signer = Signer(sighex)
    return signer.verhex.decode()


def getLocalPubKey(roleName, baseDir=None):
    prihex = getLocalRoleKeyByName(roleName, baseDir, 'prihex')
    privateer = Privateer(prihex)
    return privateer.pubhex.decode()


def getLocalEstateData(name, baseDir):
    estatePath = os.path.expanduser(os.path.join(baseDir, name, "local",
                                                 "estate.json"))
    if os.path.isfile(estatePath):
        return json.loads(open(estatePath).read())


def getHaFromLocalEstate(name, basedirpath):
    localEstate = getLocalEstateData(name, basedirpath)
    if localEstate:
        return localEstate.get("ha")


def isRaetKeepDir(directory):
    if os.path.isdir(os.path.join(directory, 'local')) and \
            os.path.isdir(os.path.join(directory, 'remote')) and \
            os.path.isdir(os.path.join(directory, 'role')):
        return True
    return False


def isPortUsedByRaetRemote(keepDir, port):
    """
    Checks if the any local remote present in `keepDir` is bound to the given
    port
    :param keepDir:
    :param port:
    :return:
    """
    for item in os.listdir(keepDir):
        itemDir = os.path.join(keepDir, item)
        if os.path.isdir(itemDir) and isRaetKeepDir(itemDir):
            try:
                localRemoteData = json.load(open(os.path.join(itemDir, 'local',
                                                              'estate.json')))
                if localRemoteData['ha'][1] == port:
                    return True
            except BaseException:
                continue
    return False
