import base64

import zmq


def create_zmq_connection(node, sock_type):
    clientstack = node.clientstack
    sock = zmq.Context().socket(sock_type)
    l_pub_key, l_sec_key = zmq.curve_keypair()
    sock.setsockopt(zmq.IDENTITY, base64.encodebytes(l_pub_key))
    sock.setsockopt(zmq.CURVE_PUBLICKEY, l_pub_key)
    sock.setsockopt(zmq.CURVE_SECRETKEY, l_sec_key)
    sock.setsockopt(zmq.TCP_KEEPALIVE, 1)

    sock.setsockopt(zmq.CURVE_SERVERKEY, clientstack.publicKey)
    sock.connect("tcp://{}:{}".format(clientstack.ha.host, clientstack.ha.port))

    return sock