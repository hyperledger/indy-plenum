from plenum.common.messages.fields import FixedLengthField
from plenum.server.plugin.did_plugin.transactions import DemoTransactions
from plenum.server.plugin.did_plugin.constants import DID_PLUGIN_LEDGER_ID


dummy_field_length = 10
LEDGER_IDS = {DID_PLUGIN_LEDGER_ID, }
CLIENT_REQUEST_FIELDS = {'fix_length_dummy':
                         FixedLengthField(dummy_field_length,
                         optional=True, nullable=True)}

AcceptableWriteTypes = {DemoTransactions.CREATE_DID.value,
                        DemoTransactions.CREATE_NETWORK_DID.value,
                        DemoTransactions.UPDATE_DID.value,
                        DemoTransactions.UPDATE_NETWORK_DID.value,
                        DemoTransactions.DEACTIVATE_DID.value
                        }

AcceptableQueryTypes = {DemoTransactions.FETCH_DID.value, }
