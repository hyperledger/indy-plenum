from plenum.common.messages.node_messages import Nomination, Reelection, Primary, Ordered, \
    Propagate, PrePrepare, Prepare, Commit, InstanceChange, Checkpoint, \
    ThreePCState, LedgerStatus, ConsistencyProof, CatchupReq, CatchupRep
from plenum.test.input_validation.fields import *
from plenum.test.input_validation.helper import *

messages = (

    # 3phase messages
    MessageDescriptor(
        klass=Propagate,
        fields=[
            client_request_field,
            sender_client_field,
        ],
    ),

    MessageDescriptor(
        klass=PrePrepare,
        fields=[
            inst_id_field,
            view_no_field,
            pp_seq_no_field,
            pp_time_field,
            req_idr_field,
            discarded_field,
            digest_field,
            ledger_id_field,
            state_root_field,
            txn_root_hash_field,
        ],
    ),

    MessageDescriptor(
        klass=Prepare,
        fields=[
            inst_id_field,
            view_no_field,
            pp_seq_no_field,
            digest_field,
            state_root_field,
            txn_root_hash_field,
        ],
    ),

    MessageDescriptor(
        klass=Commit,
        fields=[
            inst_id_field,
            view_no_field,
            pp_seq_no_field,
        ],
    ),

    MessageDescriptor(
        klass=Ordered,
        fields=[
            inst_id_field,
            view_no_field,
            req_idr_field,
            pp_seq_no_field,
            pp_time_field,
            ledger_id_field,
            state_root_field,
            txn_root_hash_field,
        ],
    ),

    # Election
    MessageDescriptor(
        klass=Nomination,
        fields=[
            name_field,
            view_no_field,
            inst_id_field,
            ord_seq_no_field,
        ],
    ),

    MessageDescriptor(
        klass=Reelection,
        fields=[
            inst_id_field,
            round_field,
            tie_among_field,
            view_no_field,
        ],
    ),

    MessageDescriptor(
        klass=Primary,
        fields=[
            name_field,
            inst_id_field,
            view_no_field,
            ord_seq_no_field,
        ],
    ),

    MessageDescriptor(
        klass=InstanceChange,
        fields=[
            view_no_field,
            reason_field,
            ord_seq_nos_field,
        ],
    ),


    MessageDescriptor(
        klass=Checkpoint,
        fields=[
            inst_id_field,
            view_no_field,
            seq_no_start_field,
            seq_no_stop_field,
            digest_field,
        ],
    ),

    MessageDescriptor(
        klass=ThreePCState,
        fields=[
            inst_id_field,
            messages_field,
        ],
    ),

    # Ledger status

    MessageDescriptor(
        klass=LedgerStatus,
        fields=[
            ledger_id_field,
            txn_seq_no_field,
            merkle_root_field,
        ],
    ),

    MessageDescriptor(
        klass=ConsistencyProof,
        fields=[
            ledger_id_field,
            seq_no_start_field,
            seq_no_stop_field,
            pp_seq_no_field,
            old_merkle_root_field,
            new_merkle_root_field,
            hashes_field,
        ],
    ),

    MessageDescriptor(
        klass=CatchupReq,
        fields=[
            ledger_id_field,
            seq_no_start_field,
            seq_no_stop_field,
            catchup_till_field,
        ],
    ),

    MessageDescriptor(
        klass=CatchupRep,
        fields=[
            ledger_id_field,
            tnxs_field,
            cons_proof_field,
        ],
    ),

    # client NODE request
    build_client_request_message(node_operation_field),

    # client NYM request
    build_client_request_message(nym_operation_field),

)

messages_names_shortcut = list(map(lambda x: x.klass.__name__, messages))
