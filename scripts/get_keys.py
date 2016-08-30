#This script takes an argument which is either Node name or client name
# and returns the public key & verification key

import json
import os
import sys
from raet.nacling import Signer, Privateer

EMPTY_STRING = ''

if(len(sys.argv) > 2):
    raise Exception('provide only one parameter which specifies node or client name')
if(len(sys.argv) < 2):
    raise Exception('provide the parameter which specifies node or client name')

NODE_OR_CLIENT_NAME = sys.argv.pop(1)
CURRENT_LOGGED_IN_USER = "sovrin"

path = '/home/'+ CURRENT_LOGGED_IN_USER +'/.plenum/'  + NODE_OR_CLIENT_NAME +'/role/local/role.json'

if not os.path.exists(path):
    raise Exception("Sorry, please check the client or node name you've entered")
with open(path, "r") as f:
    keyString = f.read().strip()
try:
    d = json.loads(keyString)
except json.decoder.JSONDecodeError:
    raise Exception("non json content exception message here")
if 'prihex' not in d:
    raise ValueError("key not defined in given data")
if 'sighex' not in d:
    raise ValueError("key not defined in given data")


prihex = d['prihex']
sighex = d['sighex']
privateer = Privateer(prihex)
pubkey = privateer.pubhex.decode()
signer = Signer(sighex)
verifkey = signer.verhex.decode()

print("\nPublic key is : " + pubkey)
print("\nVerification key is : " + verifkey)



