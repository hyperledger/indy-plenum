from plenum.common.constants import NODE, NYM
from plenum.common.transactions import PlenumTransactions


def testTransactionsAreEncoded():
    assert NODE == "0"
    assert NYM == "1"


def testTransactionEnumDecoded():
    assert PlenumTransactions.NODE.name == "NODE"
    assert PlenumTransactions.NYM.name == "NYM"


def testTransactionEnumEncoded():
    assert PlenumTransactions.NODE.value == "0"
    assert PlenumTransactions.NYM.value == "1"
