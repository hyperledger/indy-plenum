import pytest

from plenum.common.request import Request
from plenum.common.txn_util import reqToTxn, append_txn_metadata
from plenum.common.util import SortedDict


@pytest.fixture(params=['all', 'sig_only', 'sigs_only', 'no_protocol_vers'])
def req_and_expected(request):
    op = {'type': '1',
          'something': 'nothing'}
    req = Request(operation=op, reqId=1513945121191691,
                  protocolVersion=3, identifier="L5AD5g65TDQr1PPHHRoiGf")
    req.signature = "3SyRto3MGcBy1o4UmHoDezy1TJiNHDdU9o7TjHtYcSqgtpWzejMoHDrz3dpT93Xe8QXMF2tJVCQTtGmebmS2DkLS"
    req.add_signature("L5AD5g65TDQr1PPHHRoiGf",
                      "3SyRto3MGcBy1o4UmHoDezy1TJiNHDdU9o7TjHtYcSqgtpWzejMoHDrz3dpT93Xe8QXMF2tJVCQTtGmebmS2DkLS")
    if request.param == 'sig_only':
        req.signatures = None
    if request.param == 'sigs_only':
        req.signature = None
    if request.param == 'no_protocol_vers':
        req.protocolVersion = None

    new_expected = SortedDict({
        "reqSignature": {
            "type": "ED25519",
            "values": [{
                "from": "L5AD5g65TDQr1PPHHRoiGf",
                "value": "3SyRto3MGcBy1o4UmHoDezy1TJiNHDdU9o7TjHtYcSqgtpWzejMoHDrz3dpT93Xe8QXMF2tJVCQTtGmebmS2DkLS"
            }]
        },
        "txn": {
            "data": {
                "something": "nothing",
            },

            "metadata": {
                "from": "L5AD5g65TDQr1PPHHRoiGf",
                "reqId": 1513945121191691,
            },

            "protocolVersion": 3,
            "type": "1",
        },
        "txnMetadata": {
            "txnTime": 1513945121,
        },

    })

    if request.param == 'no_protocol_vers':
        new_expected["txn"]["protocolVersion"] = None

    return req, new_expected


def test_old_req_to_txn(req_and_expected):
    req, new_expected = req_and_expected
    new = SortedDict(reqToTxn(req, 1513945121))
    assert new == new_expected


def test_old_req_to_txn_with_seq_no(req_and_expected):
    req, new_expected = req_and_expected
    new = SortedDict(
        append_txn_metadata(
            reqToTxn(req, 1513945121),
            seq_no=143,
            txn_time=2613945121)
    )
    new_expected["txnMetadata"]["txnTime"] = 2613945121
    new_expected["txnMetadata"]["seqNo"] = 143
    assert new == new_expected
