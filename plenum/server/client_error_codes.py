from typing import NamedTuple

ClientError = NamedTuple("ClientError", [("code", int), ("reason", str)])


class Rejects:
    TAA_NOT_EXPECTED_FOR_LEDGER = ClientError(200,
                                              "Txn Author Agreement acceptance is not expected and not allowed in requests for ledger id {}")
    TAA_MISSING_FOR_LEDGER = ClientError(201,
                                         "Txn Author Agreement acceptance is required for ledger with id {}")
    TAA_NOT_FOUND = ClientError(202, "Incorrect Txn Author Agreement(digest={}) in the request")
    TAA_RETIRED = ClientError(203, "Txn Author Agreement is retired: version {}, seq_no {}, txn_time {}")
    TAA_TOO_PRECISE = ClientError(204, "Txn Author Agreement acceptance time {} is too precise and is a privacy risk")
    TAA_WRONG_ACCEPTANCE_TIME = ClientError(205,
                                            "Txn Author Agreement acceptance time is inappropriate: provided {}, expected in [{}, {}]")
    TAA_AML_INVALID = ClientError(206,
                                  "Txn Author Agreement acceptance mechanism is inappropriate: provided {}, expected one of {}")
    TAA_INCORRECT_ACCEPTANCE_TIME_FORMAT = ClientError(207, "TAA_ACCEPTANCE_TIME = {} is out of range")


class Nacks:
    pass
