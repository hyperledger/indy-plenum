import os
import types
from distutils.dir_util import copy_tree
from stat import ST_MODE

from stp_core.common.util import adict
from stp_core.loop.eventually import eventually
from stp_core.network.port_dispenser import genHa
from stp_core.test.helper import Printer, prepStacks, chkPrinted
from stp_zmq.util import generate_certificates
from stp_zmq.zstack import ZStack


def genKeys(baseDir, names):
    bdir = os.path.join(baseDir)
    generate_certificates(bdir, *names, clean=True)
    for n in names:
        d = os.path.join(bdir, n)
        os.makedirs(d, exist_ok=True)
        for kd in ZStack.keyDirNames():
            copy_tree(os.path.join(bdir, kd), os.path.join(d, kd))


def patch_send_ping_counter(stack):
    stack.ping_count = 0


def add_counters_to_ping_pong(stack):
    stack.sent_ping_count = 0
    stack.sent_pong_count = 0
    stack.recv_ping_count = 0
    stack.recv_pong_count = 0
    orig_send_method = stack.sendPingPong
    orig_recv_method = stack.handlePingPong

    def send_ping_pong_counter(self, remote, is_ping=True):
        if is_ping:
            self.sent_ping_count += 1
        else:
            self.sent_pong_count += 1

        return orig_send_method(remote, is_ping)

    def recv_ping_pong_counter(self, msg, frm, ident):
        if msg in (self.pingMessage, self.pongMessage):
            if msg == self.pingMessage:
                self.recv_ping_count += 1
            if msg == self.pongMessage:
                self.recv_pong_count += 1

        return orig_recv_method(msg, frm, ident)

    stack.sendPingPong = types.MethodType(send_ping_pong_counter, stack)
    stack.handlePingPong = types.MethodType(recv_ping_pong_counter, stack)


def create_and_prep_stacks(names, basedir, looper, conf):
    genKeys(basedir, names)
    printers = [Printer(n) for n in names]
    # adict is used below to copy the config module since one stack might
    # have different config from others
    stacks = [ZStack(n, ha=genHa(), basedirpath=basedir,
                     msgHandler=printers[i].print,
                     restricted=True, config=adict(**conf.__dict__))
              for i, n in enumerate(names)]
    prepStacks(looper, *stacks, connect=True, useKeys=True)
    return stacks, printers


def check_stacks_communicating(looper, stacks, printers):
    """
    Check that `stacks` are able to send and receive messages to each other
    Assumes for each stack in `stacks`, there is a printer in `printers`,
    at the same index
    """

    # Each sends the same message to all other stacks
    for idx, stack in enumerate(stacks):
        for other_stack in stacks:
            if stack != other_stack:
                stack.send({'greetings': '{} here'.format(stack.name)},
                           other_stack.name)

    # Each stack receives message from others
    for idx, printer in enumerate(printers):
        for j, stack in enumerate(stacks):
            if idx != j:
                looper.run(eventually(chkPrinted, printer,
                                      {'greetings': '{} here'.format(stack.name)}))


def get_file_permission_mask(file_path):
    return oct(os.stat(file_path)[ST_MODE] & 0o777)[-3:]


def get_zstack_key_paths(stack_name, common_path):
    home_dir = ZStack.homeDirPath(common_path, stack_name)
    # secrets
    sigDirPath = ZStack.sigDirPath(home_dir)
    secretDirPath = ZStack.secretDirPath(home_dir)
    # public
    verifDirPath = ZStack.verifDirPath(home_dir)
    pubDirPath = ZStack.publicDirPath(home_dir)
    return dict(
        secret=(
            os.path.join(sigDirPath, stack_name) + '.key_secret',
            os.path.join(secretDirPath, stack_name) + '.key_secret'
        ),
        public=(
            os.path.join(verifDirPath, stack_name) + '.key',
            os.path.join(pubDirPath, stack_name) + '.key'
        ),
    )


def check_pong_received(looper, stack, frm):
    def do_check_pong():
        assert stack.isConnectedTo(frm)

    looper.run(eventually(do_check_pong))
