import argparse
import os
import os.path

from raet.road.keeping import RoadKeep

from plenum.client.signer import SimpleSigner
from plenum.common.util import getConfig
from plenum.server.node import CLIENT_STACK_SUFFIX


config = getConfig()
keepDir = os.path.expanduser(config.keepDir)


def initKeep(name, basedir, sigseed, pkseed, override=False):
    rolePath = os.path.join(basedir, name, "role", "local", "role.json")
    if os.path.isfile(rolePath):
        if not override:
            raise Exception("Keys exists for {}".format(name))

    sSigner = SimpleSigner(sigseed)
    pSigner = SimpleSigner(pkseed)
    keep = RoadKeep(stackname=name, baseroledirpath=keepDir)
    sigkey, verkey = sSigner.naclSigner.keyhex, sSigner.naclSigner.verhex
    prikey, pubkey = pSigner.naclSigner.keyhex, pSigner.naclSigner.verhex

    keep.dumpLocalRoleData({
        "role": name,
        "prihex": prikey,
        "sighex": sigkey
    })
    print("Public key is", pubkey.decode())
    print("Verification key is", verkey.decode())

if __name__ == "__main__":
    if not os.path.exists(keepDir):
        os.makedirs(keepDir, exist_ok=True)

    parser = argparse.ArgumentParser(
        description="Generate keys for a node's stacks "
                    "by taking the node's name and 4 "
                    "seed values")

    parser.add_argument('--name', required=True, help='node name')
    parser.add_argument('--seeds', required=True, type=str, nargs=4,
                        help='seeds for keypairs')
    parser.add_argument('--force', help='overrides keys', action='store_true')
    args = parser.parse_args()
    # Initialize node stack
    print("For node stack, name is ", args.name)
    try:
        initKeep(args.name, keepDir, args.seeds[0], args.seeds[1], args.force)
    except Exception as ex:
        print(ex)
        exit()
    # Initialize client stack
    print("For client stack name is ", args.name + CLIENT_STACK_SUFFIX)
    try:
        initKeep(args.name + CLIENT_STACK_SUFFIX, keepDir, args.seeds[2],
             args.seeds[3], args.force)
    except Exception as ex:
        print(ex)
        exit()
