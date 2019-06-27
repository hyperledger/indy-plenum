import pytest as pytest

from plenum.common.constants import TXN_TYPE, GET_TXN_AUTHOR_AGREEMENT, \
    GET_TXN_AUTHOR_AGREEMENT_VERSION, GET_TXN_AUTHOR_AGREEMENT_DIGEST, GET_TXN_AUTHOR_AGREEMENT_TIMESTAMP
from plenum.common.exceptions import InvalidClientRequest
from plenum.common.request import Request
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.get_txn_author_agreement_handler import GetTxnAuthorAgreementHandler


@pytest.fixture(scope="function")
def get_txn_author_agreement_handler(tconf):
    data_manager = DatabaseManager()
    handler = GetTxnAuthorAgreementHandler(data_manager)
    return handler


def test_static_validation(get_txn_author_agreement_handler):
    request = Request(operation={TXN_TYPE: GET_TXN_AUTHOR_AGREEMENT,
                                 GET_TXN_AUTHOR_AGREEMENT_VERSION: "VERSION"})
    get_txn_author_agreement_handler.static_validation(request)

    request = Request(operation={TXN_TYPE: GET_TXN_AUTHOR_AGREEMENT,
                                 GET_TXN_AUTHOR_AGREEMENT_DIGEST: "DIGEST"})
    get_txn_author_agreement_handler.static_validation(request)

    request = Request(operation={TXN_TYPE: GET_TXN_AUTHOR_AGREEMENT,
                                 GET_TXN_AUTHOR_AGREEMENT_TIMESTAMP: 1559299045})
    get_txn_author_agreement_handler.static_validation(request)


def test_static_validation_with_redundant_fields(get_txn_author_agreement_handler):
    request = Request(operation={TXN_TYPE: GET_TXN_AUTHOR_AGREEMENT,
                                 GET_TXN_AUTHOR_AGREEMENT_VERSION: "VERSION",
                                 GET_TXN_AUTHOR_AGREEMENT_DIGEST: "DIGEST"})
    with pytest.raises(InvalidClientRequest,
                       match="GET_TXN_AUTHOR_AGREEMENT request can have at most one of "
                             "the following parameters: version, digest, timestamp"):
        get_txn_author_agreement_handler.static_validation(request)

