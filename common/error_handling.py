"""
This module provides examples for raising and handling errors
In plenum there are several general errors, which placed in plenum/common/exceptions module
"""
from common.exceptions import PlenumTypeError, PlenumValueError, LogicError
from plenum.common.exceptions import UnauthorizedClientRequest, SuspiciousNode
from plenum.common.messages.fields import IterableField
from plenum.common.request import Request
from plenum.server.suspicion_codes import Suspicions


"""--- PlenumTypeError ---
This kind of exception used for validation requests in cases when given field doesn't have needed type.
For example, according to request schema expects, that field 'a' should be iterable, but it's a usual string.
In this case the most useful behaviour is to raise PlenumTypeError with corresponding description as parameter
"""


def validate_field(field):
    if not isinstance(field, IterableField):
        # This exception will be caught up on the top level of static validation phase and
        # Reply with this message will be sent to client.
        raise PlenumTypeError("Field {} should be a variable of type {}".format(field, IterableField))


"""--- PlenumValueError ---
This kind of exception can be used in cases, when inappropriate value of field was passed.
For example, was received GET_TXN request with seq_no = -1. It's unexpected, because seq_no starts from 1 and cannot be less then 0.
In this case PlenumValueError can be raise with sufficient description
"""


def validate_value(seq_no):
    if seq_no <= 0:
        # Also, this field will be caught on static validation phase and Reply with this message will be sent to client
        raise PlenumValueError("seq_no should be number and more then 0")


"""--- UnauthorizedClientRequest ---
This kind of exception usually used in dynamic_validation process and shows,
that user cannot perform some action or write a transaction to the ledger.
We have a auth_rules mechanism for checking that user can do this and if not then Reply with corresponded error will be sent to client.
"""


def unauthorised(who_can, request: Request):
    if who_can != request.identifier:
        # This is part of dynamic validation process and after finishing ordering process (when consensus will be reached),
        # Reply with this text message will be sent to client
        raise UnauthorizedClientRequest(request.identifier, request.reqId, "This user cannot perform this action")


"""--- SuspiciousNode ---
There are a lot of reasons which can be used for defining that Node, which was sent a Node message has malicious behaviour.
In this case, if replica decides that Node is malicious it can raise SuspiciousNode exception to get information for Node instance.
Node instance will decide is it just warning (write to log) or it's critical and ViewChange procedure should be forced.
All suspicious codes can be found in module plenum/server/suspicion_codes
"""


def malicious_behaviors(suspicion_code, node_name):
    if suspicion_code == Suspicions.PPR_STATE_WRONG:
        raise SuspiciousNode(node_name,
                             suspicion_code,
                             "It looks like ViewChange should be forced "
                             "because Pre-Prepare message has incorrect state trie root")


"""--- LogicError ---
It's a kind of critical errors and refers to errors in a logic of some process.
For example, if after some steps system went to unexpected state,
then will be more safely to raise LogicError and go service down for preventing data corrupting.
This exception don't caught up and usually follows to down the service.
"""


def logic_error(current_seq_no, seq_no_from_request):
    # seq_no should be increasing sequence without jumps, like 1,2,3 not 1,3,4
    if seq_no_from_request != current_seq_no + 1:
        raise LogicError("{} is not next seq_no. Current seq_no is {}".format(seq_no_from_request, current_seq_no))
