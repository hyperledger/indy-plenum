#This script takes an argument which is either Node name or client name
# and returns the public key & verification key

print("\n=======================================================================================================================")
print("\nThis is a script to get the public key and verification key &")
print("Takes either node name or client name to get the keys\n")

import glob
import json
import os
import sys
from raet.nacling import Signer, Privateer

EMPTY_STRING = ''
NODE_OR_CLIENT_NAME = sys.argv.pop(1)
CURRENT_LOGGED_IN_USER = os.getlogin()

path = '/home/'+ CURRENT_LOGGED_IN_USER +'/.plenum/'  + NODE_OR_CLIENT_NAME +'/role/local/role.json'

if(os.path.exists(path)):

    files=glob.glob(path)
    for file in files:
        f = open(file, 'r')
        keyString = EMPTY_STRING.join(f.readlines())
        d = json.loads(keyString)
        prihex = d['prihex']
        sighex = d['sighex']
        f.close()

    privateer = Privateer(prihex)
    pubkey = privateer.pubhex.decode()

    signer = Signer(sighex)
    verifkey = signer.verhex.decode()

    print("\nPublic key is : " + pubkey)
    print("\nVerification key is : " + verifkey)

else:
    print("Sorry, please check the client or node name you've entered")

print("\n\nThank you !\n")
print("\n=======================================================================================================================")
