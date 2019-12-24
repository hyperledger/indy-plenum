# Transaction Author Agreement

#### Why It May Be Needed

- The ledger is immutable and can be public. Hence, the user will not be able to exercise the right to be forgotten, so no personal data should be published to the ledger.

- Due to legal nuances, Indy network should support flow to receive explicit confirmation from any transaction author that he or she accepts writing the data to the ledger.

- The transaction author agreement acceptance is optional, since in some cases (Indy Deployments) ledger immutability can be not an issue.

#### Transaction Author Agreement (TAA)

- Transaction Author Agreement is stored in Config Ledger.

- Every Transaction Author Agreement consists of a text, unique version and ratification timestamp. See [transactions.md](https://github.com/hyperledger/indy-node/blob/master/docs/source/transactions.md) for details.

- There could be multiple active Transaction Author Agreements in ledger.

- It's possible to set a retirement timestamp on non-latest Transaction Author Agreement.

- It's also possible to retire all Transaction Author Agreements at once (thus stopping enforcing Transaction Author Agreement on ledger).

- It's possible to get the latest Transaction Author Agreement from the ledger.

- It's also possible to query a Transaction Author Agreement by `version`, `digest` or `timestamp`.

- See [requests.md](https://github.com/hyperledger/indy-node/blob/master/docs/source/requests.md) for details.

#### Transaction Author Agreement Acceptance Mechanisms List (TAA AML)

- Transaction Author Agreement can be accepted by a number of different ways. The list of possible acceptance mechanisms is stored in Config Ledger. 

- Each Acceptance Mechanisms List has a unique version. See [transactions.md](https://github.com/hyperledger/indy-node/blob/master/docs/source/transactions.md) for details.

- It's possible to get the latest Transaction Author Agreement Acceptance Mechanisms List from the ledger.

- It's also possible to query a Transaction Author Agreement Acceptance Mechanisms List by `version`  or `timestamp`.

- See [requests.md](https://github.com/hyperledger/indy-node/blob/master/docs/source/requests.md) for details.

#### Enabling Transaction Author Agreement

- Transaction Author Agreement acceptance is never required for transactions in POOL and CONFIG ledgers.

- Transaction Author Agreement is disabled by default for all ledgers, so acceptance is not required by default.
 
- Transaction Author Agreement can be enabled for DOMAIN and plugins ledgers by setting `TRANSACTION_AUTHOR_AGREEMENT` transaction with non-empty text (see [transactions.md](https://github.com/hyperledger/indy-node/blob/master/docs/source/transactions.md)). Please note that a `TRANSACTION_AUTHOR_AGREEMENT_AML` transaction needs to be sent before sending the first `TRANSACTION_AUTHOR_AGREEMENT`.

- A new Transaction Author Agreement must have a ratification timestamp specified which can not be changed later. 
Ratification timestamp value may have any precision up to seconds.

- Newly created Transaction Author Agreement cannot have retirement timestamp.

#### Updating Transaction Author Agreement

- In order to update Transaction Author Agreement `TRANSACTION_AUTHOR_AGREEMENT` transaction should be sent, containing new version and new text of agreement. This makes it possible to use new Transaction Author Agreement, but doesn't disable previous one automatically.

- Old Transaction Author Agreements should be explicitely retired (see next section)

- Ratification timestamp can not be changed for existing Agreements after creation.  

#### Disabling Transaction Author Agreement

- Individual transaction author agreements can be disabled by setting retirement timestamp using `TRANSACTION_AUTHOR_AGREEMENT` transaction. 
Retirement timestamp can be in future, in this case deactivation of agreement won't happen immediately, it will be automatically deactivated at required time instead.

- Retirement timestamp value may have any precision up to seconds.

- It is possible to change existing retirement timestamp of agreement by sending a `TRANSACTION_AUTHOR_AGREEMENT` transaction with a new retirement timestamp.
This may potentially re-enable already retired Agreement.
Re-enabling retired Agreement needs to be considered as an exceptional case used mostly for fixing disabling by mistake or with incorrect retirement timestamp specified. 

- It is possible to delete retirement timestamp of agreement by sending a `TRANSACTION_AUTHOR_AGREEMENT` transaction without a retirement timestamp or retirement timestamp set to `None`.
This will either cancel retirement (if it hasn't occurred yet), or disable retirement of already retired transaction (re-enable the Agreement).
Re-enabling retired Agreement needs to be considered as an exceptional case used mostly for fixing disabling by mistake or with incorrect retirement timestamp specified.

- Latest transaction author agreement cannot be disabled individually.

- It is possible to disable all Transaction Author Agreements at once by sending `TRANSACTION_AUTHOR_AGREEMENT_DISABLE` transaction. 
This will immediately set current timestamp as retirement one for all not yet retired Transaction Author Agreements.

- It's not possible to re-enable an Agreement right after disabling all agreements because there is no active latest Agreement at this point.
A new Agreement needs to be sent instead.

#### Transaction Author Agreement Acceptance metadata in requests

- If Transaction Author Agreement is enabled, then all requests to DOMAIN and plugins ledgers must include Transaction Author Agreement Acceptance metadata.

- This metadata includes the acceptance mechanism, acceptance time, and the digest of the latest transaction author agreement on the ledger

- The agreement acceptance time needs to be rounded to date to prevent correlation of different transactions which is possible when acceptance time is too precise

- This metadata must be signed by the user (using a standard signature used in write requests).

- If Transaction Author Agreement is not set or disabled, then requests may have any Transaction Author Agreement Acceptance metadata.

- Requests to POOL and CONFIG ledgers must not have any Transaction Author Agreement Acceptance metadata.

- See [requests.md](https://github.com/hyperledger/indy-node/blob/master/docs/source/requests.md) for details.

#### How ledger validates Transaction Author Agreement metadata in a request

- If this is a request to POOL or CONFIG ledger, then
    
    - If the request contains Transaction Author Agreement Acceptance metadata, then REJECT it.
    
    - Otherwise - ACCEPT

- If Transaction Author Agreement is not enabled, then ACCEPT

- If Transaction Author Agreement is required, and the request doesn't have Transaction Author Agreement Acceptance metadata, then REJECT

- If Acceptance metadata's Transaction Author Agreement digest doesn't equal to the  digest of any active Transaction Author Agreement on the ledger - REJECT

  - Active TAA is an TAA either without retirement timestamp or with retirement timestamp set in future compared to batch time containing transaction. Comparison is precise (without any rounding).

- If Acceptance metadata's acceptance mechanism is not in the latest Transaction Author Agreement Acceptance Mechanisms List, then REJECT

- If Acceptance metadata's acceptance timestamp is not rounded to date, then REJECT

  - This is done to prevent correlation of different transactions which is possible when acceptance time is too precise

- If Acceptance metadata's acceptance time is not in the acceptable interval, then REJECT
  
  - The acceptable interval is defined as follows: `[date(TAA_RATIFICATION_TIME - 2 secs); date(CURRENT_TIME + 2 secs)]`, where `CURRENT_TIME` is a master Primary's time used for a 3PC batch the given request is ordered in, and `date` rounds timestamp to date

#### External Audit of Transaction Author Agreement Acceptance 

To verify Transaction Author Agreement Acceptance for a transaction written on the Ledger, an auditor should perform the following:

- Fetch origin transaction from the ledger, parse it and extract following fields:
  - digest
  - time of transaction
  - time of acceptance
  - acceptance mechanism
  
- Fetch the TAA and AML for the `time of transaction`
- Compare the `digest` of a transaction against the calculated digest of the TAA at the time of transaction
- Check membership of `acceptance mechanism` in the AML
- Verify `time of acceptance` as it's described in [verification section](#how-ledger-validates-transaction-author-agreement-metadata-in-a-request), where the `CURRENT_TIME` is `time of transaction`.
