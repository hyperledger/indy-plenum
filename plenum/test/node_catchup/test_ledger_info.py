import logging
import time
from typing import List, Tuple, Any, Optional, Callable

import pytest

from ledger.merkle_verifier import MerkleVerifier
from ledger.test.helper import random_txn, create_ledger

# noinspection PyUnresolvedReferences
from ledger.test.conftest import tempdir, txn_serializer, hash_serializer  # noqa
from plenum.common.channel import create_direct_channel
from plenum.common.ledger import Ledger
from plenum.common.ledger_info import LedgerInfo
from plenum.common.ledger_manager import LedgerManager
from plenum.common.messages.node_messages import ConsistencyProof
from plenum.common.metrics_collector import NullMetricsCollector
from plenum.server.catchup.catchup_rep_service import CatchupRepService
from plenum.server.catchup.utils import CatchupDataProvider, CatchupTill


@pytest.yield_fixture(
    scope="function",
    params=[
        'TextFileStorage',
        'ChunkedFileStorage',
        'LeveldbStorage'])
def ledger_no_genesis(request, tempdir, txn_serializer, hash_serializer):
    ledger = create_ledger(request, txn_serializer, hash_serializer, tempdir)
    yield ledger
    ledger.stop()


def create_fake_catchup_rep_service(ledger: Ledger):
    class FakeCatchupProvider(CatchupDataProvider):
        def __init__(self, ledger):
            self._ledger = ledger

        def all_nodes_names(self):
            pass

        def node_name(self) -> str:
            pass

        def ledgers(self) -> List[int]:
            return [0]

        def ledger(self, ledger_id: int) -> Ledger:
            if ledger_id == 0:
                return self._ledger

        def verifier(self, ledger_id: int) -> MerkleVerifier:
            pass

        def eligible_nodes(self) -> List[str]:
            pass

        def update_txn_with_extra_data(self, txn: dict) -> dict:
            pass

        def transform_txn_for_ledger(self, txn: dict) -> dict:
            pass

        def notify_catchup_start(self, ledger_id: int):
            pass

        def notify_catchup_complete(self, ledger_id: int):
            pass

        def notify_transaction_added_to_ledger(self, ledger_id: int, txn: dict):
            pass

        def send_to(self, msg: Any, to: str, message_splitter: Optional[Callable] = None):
            pass

        def send_to_nodes(self, msg: Any):
            pass

        def blacklist_node(self, node_name: str, reason: str):
            pass

        def discard(self, msg, reason, logMethod=logging.error, cliOutput=False):
            pass

    _, input_rx = create_direct_channel()
    provider = FakeCatchupProvider(ledger)
    service = CatchupRepService(
        ledger_id=0,
        config=None,
        input=input_rx,
        output=None,
        timer=None,
        metrics=NullMetricsCollector(),
        provider=provider)

    return service


def test_missing_txn_request(ledger_no_genesis):
    """
    Testing LedgerManager's `_missing_txns`
    """
    ledger = ledger_no_genesis
    for i in range(20):
        txn = random_txn(i)
        ledger.add(txn)

    service = create_fake_catchup_rep_service(ledger)
    assert service._num_missing_txns() == 0

    # Ledger is already ahead
    ct = CatchupTill(start_size=1, final_size=10,
                     final_hash='Gv9AdSeib9EnBakfpgkU79dPMtjcnFWXvXeiCX4QAgAC')
    service._catchup_till = ct
    service._received_catchup_txns = [(i, {}) for i in range(1, 15)]
    assert service._num_missing_txns() == 0

    # Ledger is behind but catchup replies present
    ct = CatchupTill(start_size=1, final_size=30,
                     final_hash='EEUnqHf2GWEpvmibiXDCZbNDSpuRgqdvCpJjgp3KFbNC')
    service._catchup_till = ct
    service._received_catchup_txns = [(i, {}) for i in range(21, 31)]
    assert service._num_missing_txns() == 0
    service._received_catchup_txns = [(i, {}) for i in range(21, 35)]
    assert service._num_missing_txns() == 0

    # Ledger is behind
    ct = CatchupTill(start_size=1, final_size=30,
                     final_hash='EEUnqHf2GWEpvmibiXDCZbNDSpuRgqdvCpJjgp3KFbNC')
    service._catchup_till = ct
    service._received_catchup_txns = [(i, {}) for i in range(21, 26)]
    assert service._num_missing_txns() == 5

    service._received_catchup_txns = [(i, {}) for i in range(26, 31)]
    assert service._num_missing_txns() == 5
