# Transaction Author Agreement

#### Why It May Be Needed

- The ledger is immutable and can be public. Hence, the user will not be able to exercise the right to be forgotten, so no personal data should be published to the ledger.

- Due to legal nuances, Indy network should support flow to receive explicit confirmation from any transaction author that he or she accepts writing the data to the ledger.

- The transaction author agreement acceptance is optional, since in some cases (Indy Deployments) ledger immutability can be not an issue.

#### Transaction Author Agreement (TAA)

- Transaction Author Agreement is stored in Config Ledger.

- Transaction Author Agreement consists of a text and a unique version. See [transactions.md](https://github.com/hyperledger/indy-node/blob/master/docs/source/transactions.md) for details.

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
 
- Transaction Author Agreement can be enabled for DOMAIN and plugins ledgers by setting `TRANSACTION_AUTHOR_AGREEMENT` transaction with non-empty text (see [transactions.md](https://github.com/hyperledger/indy-node/blob/master/docs/source/transactions.md)). Please note, that a `TRANSACTION_AUTHOR_AGREEMENT_AML` transaction needs to be sent before sending the first `TRANSACTION_AUTHOR_AGREEMENT`.

#### Disabling Transaction Author Agreement

- Transaction Author Agreement can be disabled for DOMAIN and plugins ledgers by setting `TRANSACTION_AUTHOR_AGREEMENT` transaction with an empty text.

#### Transaction Author Agreement Acceptance metadata in requests

- If Transaction Author Agreement is enabled, then all requests to DOMAIN and plugins ledgers must include Transaction Author Agreement Acceptance metadata.

- This metadata includes the acceptance mechanism, acceptance time, and the digest of the latest transaction author agreement on the ledger

- This metadata must be signed by the user (using a standard signature used in write requests).

- If Transaction Author Agreement is not set or disabled, then requests must not have any Transaction Author Agreement Acceptance metadata. 

- Requests to POOL and CONFIG ledgers must not have any Transaction Author Agreement Acceptance metadata.

- See [requests.md](https://github.com/hyperledger/indy-node/blob/master/docs/source/requests.md) for details.

#### How ledger validates Transaction Author Agreement metadata in a request

- If Transaction Author Agreement is not enabled, or if this is a request to POOL or CONFIG ledger, then
    
    - If the request contains Transaction Author Agreement Acceptance metadata, then REJECT it.
    
    - Otherwise - ACCEPT

- If Transaction Author Agreement is required, and the request doesn't have Transaction Author Agreement Acceptance metadata, then REJECT

- If Acceptance metadata's Transaction Author Agreement digest doesn't equal to the  digest of the latest Transaction Author Agreement on the ledger - REJECT

- If Acceptance metadata's acceptance mechanism is not in the latest Transaction Author Agreement Acceptance Mechanisms List, then REJECT

- If Acceptance metadata's acceptance time is not in the acceptable interval, then REJECT
  
  - The acceptable interval is defined as follows: `[LATEST_TAA_TXN_TIME - 2 secs; CURRENT_TIME + 2 secs]`, where `CURRENT_TIME` is a master Primary's time used for a 3PC batch the given request is ordered in. 

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
