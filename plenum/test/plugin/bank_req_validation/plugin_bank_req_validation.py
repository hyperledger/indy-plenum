from plenum.common.constants import TXN_TYPE, DATA
from plenum.common.types import PLUGIN_TYPE_VERIFICATION

CREDIT = "CREDIT"
GET_BAL = "GET_BAL"
GET_ALL_TXNS = "GET_ALL_TXNS"
AMOUNT = "amount"


class BankReqValidationPlugin:
    pluginType = PLUGIN_TYPE_VERIFICATION

    validTxnTypes = [CREDIT, GET_BAL, GET_ALL_TXNS]

    def __init__(self):
        self.count = 0

    def verify(self, operation):
        typ = operation.get(TXN_TYPE)
        assert typ in self.validTxnTypes, \
            "{} is not a valid transaction type, must be one of {}".\
            format(typ, ', '.join(self.validTxnTypes))

        if typ == CREDIT:
            data = operation.get(DATA)
            assert isinstance(data, dict), \
                "{} attribute is missing or not in proper format".format(DATA)
            amount = data.get(AMOUNT)
            assert isinstance(amount, (int, float)) and amount > 0, \
                "{} must be present and should be a number greater than 0"\
                .format(AMOUNT)
        self.count += 1
