from plenum import PLUGIN_CLIENT_REQUEST_FIELDS
from plenum.common.constants import NODE_IP, NODE_PORT, CLIENT_IP, \
    CLIENT_PORT, ALIAS, SERVICES, TXN_TYPE, DATA, \
    TARGET_NYM, VERKEY, ROLE, NODE, NYM, GET_TXN, VALIDATOR, BLS_KEY, OPERATION_SCHEMA_IS_STRICT
from plenum.common.messages.fields import NetworkIpAddressField, \
    NetworkPortField, IterableField, \
    ChooseField, ConstantField, DestNodeField, VerkeyField, DestNymField, \
    RoleField, TxnSeqNoField, IdentifierField, \
    NonNegativeNumberField, SignatureField, MapField, LimitedLengthStringField, \
    ProtocolVersionField, LedgerIdField, Base58Field
from plenum.common.messages.message_base import MessageValidator
from plenum.common.types import OPERATION, f
from plenum.config import ALIAS_FIELD_LIMIT, DIGEST_FIELD_LIMIT, \
    SIGNATURE_FIELD_LIMIT, BLS_KEY_LIMIT


class ClientNodeOperationData(MessageValidator):
    schema = (
        (NODE_IP, NetworkIpAddressField(optional=True)),
        (NODE_PORT, NetworkPortField(optional=True)),
        (CLIENT_IP, NetworkIpAddressField(optional=True)),
        (CLIENT_PORT, NetworkPortField(optional=True)),
        (ALIAS, LimitedLengthStringField(max_length=ALIAS_FIELD_LIMIT)),
        (SERVICES, IterableField(ChooseField(values=(VALIDATOR,)), optional=True)),
        (BLS_KEY, Base58Field(byte_lengths=(128,), optional=True)),
    )

    def _validate_message(self, dct):
        # TODO: make ha fields truly optional (needs changes in stackHaChanged)
        required_ha_fields = {NODE_IP, NODE_PORT, CLIENT_IP, CLIENT_PORT}
        ha_fields = {f for f in required_ha_fields if f in dct}
        if ha_fields and len(ha_fields) != len(required_ha_fields):
            self._raise_missed_fields(*list(required_ha_fields - ha_fields))


class ClientNodeOperation(MessageValidator):
    schema = (
        (TXN_TYPE, ConstantField(NODE)),
        (DATA, ClientNodeOperationData()),
        (TARGET_NYM, DestNodeField()),
        (VERKEY, VerkeyField(optional=True)),
    )


class ClientNYMOperation(MessageValidator):
    schema = (
        (TXN_TYPE, ConstantField(NYM)),
        (ALIAS, LimitedLengthStringField(max_length=ALIAS_FIELD_LIMIT, optional=True)),
        (VERKEY, VerkeyField(optional=True, nullable=True)),
        (TARGET_NYM, DestNymField()),
        (ROLE, RoleField(optional=True)),
        # TODO: validate role using ChooseField,
        # do roles list expandable form outer context
    )


class ClientGetTxnOperation(MessageValidator):
    schema = (
        (TXN_TYPE, ConstantField(GET_TXN)),
        (f.LEDGER_ID.nm, LedgerIdField(optional=True)),
        (DATA, TxnSeqNoField()),
    )


class ClientOperationField(MessageValidator):

    def __init__(self, *args, **kwargs):
        strict = kwargs.get("schema_is_strict", OPERATION_SCHEMA_IS_STRICT)
        self.operations = {
            NODE: ClientNodeOperation(schema_is_strict=strict),
            NYM: ClientNYMOperation(schema_is_strict=strict),
            GET_TXN: ClientGetTxnOperation(schema_is_strict=strict),
        }
        super().__init__(*args, **kwargs)

    def validate(self, dct):
        """
        Choose a schema for client request operation and validate
        the operation field. If the schema is not found skips validation.
        :param dct: an operation field from client request
        :return: raises exception if invalid request
        """
        if not isinstance(dct, dict):
            # TODO this check should be in side of the validator not here
            self._raise_invalid_fields('', dct, 'wrong type')
        txn_type = dct.get(TXN_TYPE)
        if txn_type is None:
            self._raise_missed_fields(TXN_TYPE)
        if txn_type in self.operations:
            # check only if the schema is defined
            op = self.operations[txn_type]
            op.validate(dct)


class ClientMessageValidator(MessageValidator):
    schema = (
        (f.IDENTIFIER.nm, IdentifierField(optional=True, nullable=True)),
        (f.REQ_ID.nm, NonNegativeNumberField()),
        (OPERATION, ClientOperationField()),
        (f.SIG.nm, SignatureField(max_length=SIGNATURE_FIELD_LIMIT,
                                  optional=True)),
        (f.DIGEST.nm, LimitedLengthStringField(max_length=DIGEST_FIELD_LIMIT,
                                               optional=True)),
        (f.PROTOCOL_VERSION.nm, ProtocolVersionField()),
        (f.SIGS.nm, MapField(IdentifierField(),
                             SignatureField(max_length=SIGNATURE_FIELD_LIMIT),
                             optional=True, nullable=True)),
    )

    def __init__(self, operation_schema_is_strict, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Following code is for support of non-strict schema
        # TODO: refactor this
        # TODO: this (and all related functionality) can be removed when
        # when fixed problem with transaction serialization (INDY-338)
        # Adding fields from enabled plugins to schema.
        self.schema = self.schema + tuple(PLUGIN_CLIENT_REQUEST_FIELDS.items())
        if operation_schema_is_strict:
            operation_field_index = 2
            op = ClientOperationField(schema_is_strict=operation_schema_is_strict)
            schema = list(self.schema)
            schema[operation_field_index] = (OPERATION, op)
            self.schema = tuple(schema)

    def validate(self, dct):
        super().validate(dct)
        if not (dct.get(f.IDENTIFIER.nm) or dct.get(f.SIGS.nm)):
            self._raise_invalid_message(
                'Missing both signatures and identifier')
