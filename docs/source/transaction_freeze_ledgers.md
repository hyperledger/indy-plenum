# Transaction Freeze Ledgers

#### Why It May Be Needed

- Removing of deprecated ledgers need of painless mechanism. Transaction freeze ledgers give a security transition. Transaction Freeze Ledgers disable specific ledgers (system ignore frozen ledgers).
Some ledgers can become deprecated and outdated. By means of auth map we could prohibit actions with these ledgers. Cons of this approach:
- it will not be obvious to the user that the ledger is out of date and why the transactions stopped being recorded
- the ledger can be returned to work, which in view of the long downtime can cause technical and business issues.
- the ledger cannot be removed from the system and will require to spend money on resource allocation and support of the outdated database.
To get rid of these drawbacks, a ledger freeze transaction was created. It allows to completely freeze the ledger and remove it depending on the steward's desire.

#### Transaction Freeze Ledgers (TFL)

- LEDGERS_FREEZE is transaction for add specific ledgers to list of frozen ledgers.

- Frozen ledgers can't be unfreeze. After freeze you can delete frozen ledger via [remove_ledger.py] (https://github.com/hyperledger/indy-node/blob/master/scripts/remove_ledger.py).

- Base ledgers POOL, CONFIG, AUDIT and DOMAIN can't be frozen.

- Every transaction LEDGERS_FREEZE consists of a list ledgers ids, unique version.

- It's possible to get the whole list of frozen ledgers via transaction GET_FROZEN_LEDGERS.

- See [hyperledger/README.md](https://github.com/esplinr/indy-hipe/blob/master/text/0162-frozen-ledgers/README.md) and [sovrin/README.md](https://github.com/esplinr/indy-hipe/blob/sovrin-sip/text/5005-token-removal/README.md) for details.

#### How it affects the system

- Frozen ledgers are not catched up (because the LEDGERS_FREEZE transaction is part of the config ledger, which is applied before additional ledgers).

- Do not check, do not send new batches to update frozen ledgers.

- Recording continues in audit transactions, but without using real ledgers and using data about frozen ledgers from the config state.

- Any transactions to frozen ledgers are forbidden.