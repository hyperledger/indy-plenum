from typing import NamedTuple

Environment = NamedTuple("Environment", [
    ("poolLedger", str),
    ("domainLedger", str)
])


ENVS = {
    "test": Environment("pool_transactions_sandbox",
                        "transactions_sandbox"),
    "live": Environment("pool_transactions_live",
                        "transactions_live")
}