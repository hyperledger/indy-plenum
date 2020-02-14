#! /usr/bin/env python3

import argparse
import shutil
import socket
from concurrent.futures import ThreadPoolExecutor
from tempfile import TemporaryDirectory
from typing import NamedTuple

import zmq
from test_zmq.zstack import ZStack


SEED = b'E21bEA7DeaE981cBabCECd9FAeF4e340'
EXPECTED_ZMQ_REPLY = '{"type": "DIAGNOSE", "text": "ZMQ connection is possible"}'
EXPECTED_TCP_REPLY = '{"type": "DIAGNOSE", "text": "TCP connection is possible"}'
QUIT = False

HA = NamedTuple("HA", [
    ("host", str),
    ("port", int)])


def msg_handler(zstack, msg):
    _, frm = msg
    print(msg)
    zstack.send(EXPECTED_ZMQ_REPLY, frm)


def loop():
    loop = zmq.asyncio.ZMQEventLoop()
    return loop


class SafeTemporaryDirectory(TemporaryDirectory):

    @classmethod
    def _cleanup(cls, name, warn_message):
        shutil.rmtree(name, ignore_errors=True)

    def cleanup(self):
        if self._finalizer.detach():
            shutil.rmtree(self.name, ignore_errors=True)


def up_zmq_server(server_ha):
    print("ZMQ_SERVER: Seed is {}".format(SEED))
    server = ZStack(name='Test_zmq',
                    ha=server_ha,
                    basedirpath=base_dir,
                    msgHandler=msg_handler,
                    seed=SEED,
                    onlyListener=True)
    server.start()
    return server


def up_tcp_server(server_ha):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(server_ha)
        s.listen()
        print("TCP_SERVER: Listen clients on {}".format(server_ha))
        while True:
            conn, addr = s.accept()
            with conn:
                print('TCP_SERVER: Connected by', addr)
                while True:
                    data = conn.recv(1024)
                    if data:
                        print("TCP_SERVER: Received {} from client through tcp".format(data))
                        conn.sendall(EXPECTED_TCP_REPLY.encode())
                        break


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--zmq_port', help="Port which will be used for ZMQ client's connections")
    parser.add_argument('--tcp_port', help="Port which will be used for TCP client's connections")
    parser.add_argument('--addr', help="Address which will used for incoming client's connection. 0.0.0.0 by default",
                        default='0.0.0.0', required=False)
    args = parser.parse_args()

    zmq_server_ha = HA(args.addr,
                       int(args.zmq_port) if args.zmq_port else '9999')
    tcp_server_ha = HA(args.addr,
                       int(args.tcp_port) if args.tcp_port else 10000)

    with SafeTemporaryDirectory() as base_dir:

        zmq_server = up_zmq_server(zmq_server_ha)
        tpe = ThreadPoolExecutor(max_workers=4)
        tpe.submit(up_tcp_server, tcp_server_ha)

        async def wrapper():
            while True:
                await zmq_server.service()

        looper = loop()
        try:
            looper.run_until_complete(wrapper())
        except KeyboardInterrupt:
            zmq_server.stop()
            tpe.shutdown(wait=False)
            print("Server was stopped")
            exit(0)
