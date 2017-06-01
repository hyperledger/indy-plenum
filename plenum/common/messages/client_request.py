from plenum.common.constants import *
from plenum.common.messages.fields import *
from plenum.common.messages.message_base import MessageValidator


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
        required_ha_fields = {NODE_IP, NODE_PORT, CLIENT_IP, CLIENT_PORT}
        ha_fields = {f for f in required_ha_fields if f in dct}
        if ha_fields and len(ha_fields) != len(required_ha_fields):
            self._raise_missed_fields(*list(required_ha_fields - ha_fields))


class ClientNodeOperation(MessageValidator):
    schema = (
        (TXN_TYPE, ConstantField(NODE)),
        (DATA, ClientNodeOperationData()),
        (TARGET_NYM, IdentifierField()),
        (VERKEY, VerkeyField(optional=True)),
    )


class ClientNYMOperation(MessageValidator):
    schema = (
        (TXN_TYPE, ConstantField(NYM)),
        (ALIAS, NonEmptyStringField(optional=True)),
        (VERKEY, VerkeyField(optional=True)),
        (TARGET_NYM, IdentifierField()),
        (ROLE, RoleField(optional=True)),
        # TODO: validate role using ChooseField,
        # do roles list expandable form outer context
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
        if not schema_type:
            self._raise_missed_fields(TXN_TYPE)
        if schema_type in self.operations:
            # check only if the schema is defined
            op = self.operations[schema_type]
            self._validate_fields_with_schema(dct, op.schema)
            self._validate_message(dct)
