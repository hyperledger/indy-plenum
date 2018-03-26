from typing import NamedTuple

Identifier = str
HA = NamedTuple("HA", [
    ("host", str),
    ("port", int)])
