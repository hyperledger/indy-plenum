import os
import sys

from plenum.common.looper import Looper
from plenum.common.raet import initRemoteKeep
from plenum.common.util import getConfig
from plenum.server.node import Node

config = getConfig()
keepDir = os.path.expanduser(config.keepDir)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Provide a name for the node")
        exit()
    else:
        selfName = sys.argv[1]
        if selfName not in config.nodeReg:
            print("Provide a name for the node that exists in the node regsitry")
            exit()
        else:
            for remoteName, ((host, port), verkey, pubkey) in config.nodeReg.items():
                if remoteName != selfName:
                    try:
                        initRemoteKeep(selfName, remoteName, keepDir, pubkey, verkey)
                    except Exception as ex:
                        print(ex)
            with Looper(debug=True) as looper:
                node = Node(selfName, nodeRegistry=config.nodeReg, basedirpath=keepDir)
                looper.add(node)
                looper.run()


