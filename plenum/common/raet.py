import os
from collections import OrderedDict

from raet.nacling import Signer, Privateer
from raet.road.keeping import RoadKeep

from plenum.client.signer import SimpleSigner


def initLocalKeep(name, baseDir, pkseed, sigseed, override=False):
    rolePath = os.path.join(baseDir, name, "role", "local", "role.json")
    if os.path.isfile(rolePath):
        if not override:
            raise Exception("Keys exists for local role {}".format(name))

    if not isinstance(pkseed, bytes):
        pkseed = pkseed.encode()
    if not isinstance(sigseed, bytes):
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
    rolePath = os.path.join(baseDir, name, "role", "remote", "role.{}.json".
                            format(remoteName))
    if os.path.isfile(rolePath):
        if not override:
            raise Exception("Keys exists for remote role {}".format(remoteName))

    keep = RoadKeep(stackname=name, baseroledirpath=baseDir)
    data = OrderedDict([
        ('role', remoteName),
        ('acceptance', 1),
        ('pubhex', pubkey),
        ('verhex', verkey)
    ])
    keep.dumpRemoteRoleData(data, role=remoteName)
