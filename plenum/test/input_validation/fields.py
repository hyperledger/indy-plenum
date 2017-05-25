from plenum.common.request import SafeRequest
from plenum.test.input_validation.helper import *

name_field = NonEmptyStringField('name')

view_no_field = PositiveNumberField('viewNo')

inst_id_field = PositiveNumberField('instId')

ord_seq_no_field = PositiveNumberField('ordSeqNo')

round_field = PositiveNumberField('round')

tie_among_field = ListField('tieAmong', TieAmongField())

req_idr_field = RequestIdrField("reqIdr")

pp_seq_no_field = PositiveNumberField('ppSeqNo')

pp_time_field = TimestampField('ppTime')

ledger_id_field = LedgerIdFiled("ledgerId")

state_root_field = MerkleRootField('stateRootHash')

txn_root_hash_field = MerkleRootField("txnRootHash")

sender_client_field = NonEmptyStringField('senderClient')

discarded_field = PositiveNumberField('discarded')

digest_field = NonEmptyStringField('digest')

reason_field = PositiveNumberField('reason')

ord_seq_nos_field = ListField('ordSeqNos', PositiveNumberField())

seq_no_start_field = PositiveNumberField('seqNoStart')

seq_no_stop_field = PositiveNumberField('seqNoEnd')

txn_seq_no_field = PositiveNumberField('txnSeqNo')

merkle_root_field = MerkleRootField('merkleRoot')

old_merkle_root_field = MerkleRootField('oldMerkleRoot')

new_merkle_root_field = MerkleRootField('newMerkleRoot')

hashes_field = ListField('hashes', HexString64Field())

catchup_till_field = PositiveNumberField('catchupTill')

cons_proof_field = ListField('consProof', HexString64Field())

identifier_field = IdentifierField('identifier')

req_id_field = PositiveNumberField('reqId')

signature_field = SignatureField('signature')

node_port_field = NetworkPortField('node_port')

client_port_field = NetworkPortField('client_port')

node_ip_field = NetworkIpAddressField('node_ip')

client_ip_field = NetworkIpAddressField('client_ip')

alias_field = NonEmptyStringField('alias')

services_field = ServicesNodeOperation('services')

dest_field = IdentifierField('dest')

verkey_field = VerkeyField('verkey')

role_field = RoleField('role')


# creates node operation field
client_node_op_data = MessageDescriptor(
        dict,
        fields=[
            node_port_field,
            client_port_field,
            node_ip_field,
            client_ip_field,
            alias_field,
            services_field,
        ],
        name='data',
        optional_fields=(node_port_field, client_port_field, node_ip_field,
                         client_ip_field, alias_field, services_field),
    )


def create_nym_op(name=None):
    return MessageDescriptor(
        dict,
        fields=[
            ConstantField('type', '1'),
            alias_field,
            verkey_field,
            dest_field,
            role_field,
        ],
        name=name,
    )


def create_node_op(name=None):
    return MessageDescriptor(
        dict,
        fields=[
            ConstantField('type', '0'),
            dest_field,
            client_node_op_data,
        ],
        name=name
    )


def build_client_request_message(op_field, name=None):
    return MessageDescriptor(
        klass=SafeRequest,
        fields=[
            identifier_field,
            req_id_field,
            op_field,
            signature_field,
        ],
        optional_fields=(signature_field,),
        name=name
    )


# check complex field using NODE op
node_operation_field = create_node_op('operation')

nym_operation_field = create_nym_op('operation')

client_request_field = build_client_request_message(create_node_op('operation'), 'request')

tnxs_field = ListField('txns', build_client_request_message(create_node_op('operation')))

messages_field = ListField('messages', build_client_request_message(create_node_op('operation')))
