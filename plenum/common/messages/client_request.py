from plenum.common.constants import *
from plenum.common.messages.fields import *
from plenum.common.messages.message_base import MessageValidator
from plenum.common.types import OPERATION, f


class ClientNodeOperationData(MessageValidator):
    schema = (
        (NODE_IP, NetworkIpAddressField(optional=True)),
        (NODE_PORT, NetworkPortField(optional=True)),
        (CLIENT_IP, NetworkIpAddressField(optional=True)),
        (CLIENT_PORT, NetworkPortField(optional=True)),
        (ALIAS, NonEmptyStringField()),
        (SERVICES, IterableField(ChooseField(values=(VALIDATOR,)), optional=True)),
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
        (ALIAS, NonEmptyStringField(optional=True)),
        (VERKEY, VerkeyField(optional=True)),
        (TARGET_NYM, DestNymField()),
        (ROLE, RoleField(optional=True)),
        # TODO: validate role using ChooseField,
        # do roles list expandable form outer context
    )
    schema_is_strict = False


class ClientGetTxnOperation(MessageValidator):
    schema = (
        (TXN_TYPE, ConstantField(GET_TXN)),
        (DATA, TxnSeqNoField()),
    )


class ClientOperationField(MessageValidator):

    def __init__(self, *args, **kwargs):
        strict = kwargs.get("schema_is_strict", True)
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
        schema_type = dct.get(TXN_TYPE, None)
        if not schema_type:
            self._raise_missed_fields(TXN_TYPE)
        if schema_type in self.operations:
            # check only if the schema is defined
            op = self.operations[schema_type]
            op.validate(dct)


class ClientMessageValidator(MessageValidator):

    def __init__(self, operation_schema_is_strict, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Following code is for support of non-strict schema
        # TODO: refactor this
        # TODO: this (and all related functionality) can be removed when
        # when fixed problem with transaction serialization (INDY-338)
        strict = operation_schema_is_strict
        if not strict:
            operation_field_index = 2
            op = ClientOperationField(schema_is_strict=False)
            schema = list(self.schema)
            schema[operation_field_index] = (OPERATION, op)
            self.schema = tuple(schema)

    schema = (
        (f.IDENTIFIER.nm, IdentifierField()),
        (f.REQ_ID.nm, NonNegativeNumberField()),
        (OPERATION, ClientOperationField()),
        (f.SIG.nm, SignatureField(optional=True)),
        (f.DIGEST.nm, NonEmptyStringField(optional=True)),
    )