#! /usr/bin/env python3

import base64
import ctypes
import socket
from binascii import hexlify, unhexlify

import select
import zmq
from zmq.utils import z85
import base58
from libnacl import nacl, crypto_box_PUBLICKEYBYTES

import argparse
from test_zmq.crypto.nacl_wrappers import SigningKey, Signer as NaclSigner

DEST = 'Gw6pDLhcBcoQesN72qfotTgFa7cbuqZpkX3Xo6pLhPhv'
SERVER_SEED = b'E21bEA7DeaE981cBabCECd9FAeF4e340'
TEST_MSG = '{"type": "DIAGNOSE", "text": "Check connection message"}'
EXPECTED_ZMQ_REPLY = '{"type": "DIAGNOSE", "text": "ZMQ connection is possible"}'
EXPECTED_TCP_REPLY = '{"type": "DIAGNOSE", "text": "TCP connection is possible"}'
ZMQ_NETWORK_PROTOCOL = 'tcp'
WAIT_TIMEOUT = 10


def rawToFriendly(raw):
    return base58.b58encode(raw).decode("utf-8")


def hexToFriendly(hx):
    if isinstance(hx, str):
        hx = hx.encode()
    raw = unhexlify(hx)
    return rawToFriendly(raw)


def ed_25519_pk_to_curve_25519(pk, to_hex=False):
    pub_key = ctypes.create_string_buffer(crypto_box_PUBLICKEYBYTES)
    ret = nacl.crypto_sign_ed25519_pk_to_curve25519(pub_key, pk)
    if ret:
        raise Exception("error in converting ed22519 key to curve25519")
    return hexlify(pub_key.raw) if to_hex else pub_key.raw


def check_tcp_connection(addr_port):
    sock = socket.socket()
    addr, port = addr_port.split(':')
    try:
        sock.connect((addr, int(port)))
        sock.send(TEST_MSG.encode())
    except OSError as e:
        print("TCP CHECK PHASE::: Cannot connect to {} because\n {}".format(addr_port, e))
        return False

    print("TCP CHECK PHASE:::Waiting {} seconds for response from server".format(WAIT_TIMEOUT))
    ready = select.select([sock], [], [], WAIT_TIMEOUT)
    if ready[0]:
        reply = sock.recv(1024)
        reply = reply.decode()
        print("TCP CHECK PHASE::: Got reply {}".format(reply))

        if reply != EXPECTED_TCP_REPLY:
            print("TCP CHECK PHASE:::TCP connection test failed. \nGot {} \nbut expected reply {}\n".format(reply, EXPECTED_TCP_REPLY))
        print("TCP CHECK PHASE:::Got expected response")
        return True
    return False


def get_dest(seed):
    sk = SigningKey(seed=seed)
    naclSigner = NaclSigner(sk)
    hex_verkey = hexlify(naclSigner.verraw)
    verkey = hexToFriendly(hex_verkey)
    return verkey


def check_zmq_connection(addr_port):
    ctx = zmq.Context()
    sock = ctx.socket(zmq.DEALER)
    l_pub_key, l_sec_key = zmq.curve_keypair()
    sock.setsockopt(zmq.IDENTITY, base64.encodebytes(l_pub_key))
    sock.setsockopt(zmq.CURVE_PUBLICKEY, l_pub_key)
    sock.setsockopt(zmq.CURVE_SECRETKEY, l_sec_key)
    dest = get_dest(SERVER_SEED)
    sock.setsockopt(zmq.CURVE_SERVERKEY, z85.encode(ed_25519_pk_to_curve_25519(base58.b58decode(dest))))

    try:
        sock.connect("{}://{}".format(ZMQ_NETWORK_PROTOCOL, addr_port))
        sock.send_string(TEST_MSG)
    except OSError as e:
        print("ZMQ CHECK PHASE::: Cannot connect to {} because\n {}".format(addr_port, e))
        return False

    print("ZMQ CHECK PHASE:::Waiting {} seconds for response from server".format(WAIT_TIMEOUT))

    ready = zmq.select([sock], [], [], WAIT_TIMEOUT)
    if ready[0]:
        reply = sock.recv(flags=zmq.NOBLOCK)
        reply = reply.decode()
        print("ZMQ CHECK PHASE::: Got reply {}".format(reply))

        if reply != EXPECTED_ZMQ_REPLY:
            print("ZMQ CHECK PHASE:::ZMQ connection test failed. \nGot {} \nbut expected reply {}\n".format(reply, EXPECTED_ZMQ_REPLY))
        print("ZMQ CHECK PHASE:::Got expected response")
        return True
    return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--zmq',
                        help="String with address:port representation. Like 127.0.0.1:9702",
                        required=True)
    parser.add_argument('--tcp',
                        help="String with address:port representation. Like 127.0.0.1:10000",
                        required=False)

    args = parser.parse_args()

    if not check_tcp_connection(args.tcp):
        print("TCP CONNECTION IS NOT POSSIBLE")
        exit(1)
    print("TCP connection check finished and it's possible")

    if not check_zmq_connection(args.zmq):
        print("ZMQ CONNECTION IS NOT POSSIBLE. Sorry")
        exit(1)

    print("Looks like ZMQ connection is possible")
    exit(0)
