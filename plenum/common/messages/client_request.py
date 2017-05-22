from plenum.common.constants import *
from plenum.common.messages.fields import *
from plenum.common.messages.message_base import MessageValidator, MessageBase
from plenum.common.types import f, OPERATION


class ClientNodeOperationData(MessageValidator):
    schema = (
        (NODE_IP, NetworkIpAddressField()),
        (NODE_PORT, NetworkPortField()),
        (CLIENT_IP, NetworkIpAddressField()),
        (CLIENT_PORT, NetworkPortField()),
        (ALIAS, NonEmptyStringField()),
        (SERVICES, IterableField(ChooseField(values=(VALIDATOR,)))),
    )


class ClientNodeOperation(MessageValidator):
    schema = (
        (TXN_TYPE, ConstantField(NODE)),
        (DATA, ClientNodeOperationData()),
        (TARGET_NYM, IdentifierField()),
    )


class ClientNYMOperation(MessageValidator):
    schema = (
        (TXN_TYPE, ConstantField(NYM)),
        (ALIAS, NonEmptyStringField()),
        (VERKEY, VerkeyField()),
        (TARGET_NYM, IdentifierField()),
        (ROLE, ChooseField([Roles.TRUSTEE.value, Roles.STEWARD.value])),
    )


class ClientOperationField(MessageValidator):

    operations = {
        NODE: ClientNodeOperation(),
        NYM: ClientNYMOperation(),
    }

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
        if schema_type in self.operations:
            # check only if there is a schema
            op = self.operations.get(schema_type, None)
            if op:
                self._validate_with_schema(dct, op.schema)


class ClientMessageValidator(MessageValidator):
    schema = (
        (f.IDENTIFIER.nm, IdentifierField()),
        (f.REQ_ID.nm, NonNegativeNumberField()),
        (OPERATION, ClientOperationField()),
        (f.SIG.nm, SignatureField(optional=True)),
    )
