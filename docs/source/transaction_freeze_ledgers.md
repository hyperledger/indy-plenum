# Transaction Freeze Ledgers

#### Why It May Be Needed

- Removing tokens need of mechanism of painless way. Transaction freeze ledgers give a security transition. Transaction Freeze Ledgers disable specific ledgers (system ignore frozen ledgers).

#### Transaction Freeze Ledgers (TFL)

- LEDGERS_FREEZE is transaction for add specific ledgers to list of frozen ledgers.

- Every transaction LEDGERS_FREEZE consists of a list ledgers ids, unique version.

- It's possible to get the whole list of frozen ledgers via transaction GET_FROZEN_LEDGERS.

- See [hyperledger/README.md](https://github.com/esplinr/indy-hipe/blob/master/text/0162-frozen-ledgers/README.md) and [sovrin/README.md](https://github.com/esplinr/indy-hipe/blob/sovrin-sip/text/5005-token-removal/README.md) for details.