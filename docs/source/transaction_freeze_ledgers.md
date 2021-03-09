# Freeze Ledgers


#### Background

The Freeze Ledgers transaction implements the functionality proposed in [Indy HIPE 0162 Frozen Ledgers](https://github.com/hyperledger/indy-hipe/blob/master/text/0162-frozen-ledgers/README.md). That HIPE contains useful information for understanding this feature.


#### Key Information

- LEDGERS_FREEZE is a transaction for adding specific ledgers to a list of frozen ledgers where the ledgers can be read but cannot be written.

- Frozen ledgers can't be unfrozen.

- A frozen ledger can be removed using the [remove_ledger script] (https://github.com/hyperledger/indy-node/blob/master/scripts/remove_ledger.py), but beware that removing a ledger will delete its data. It can no longer be used for reading or writing. A production ledger with history should never be removed as this could make it impossible to reconstruct the history of the network. 

- The base ledgers cannot be frozen. These are POOL, CONFIG, AUDIT and DOMAIN.

- The LEDGERS_FREEZE transaction consists of a list of ledgers ids and unique version.

- To get the list of previously frozen ledgers, use the transaction GET_FROZEN_LEDGERS.


#### Why It May Be Needed

- During development and testing, it may be necessary to remove a ledger without recreating the data on all the other ledgers. By freezing the ledger, this can be done in safe manner.

- It may become necessary to deprecate a production ledger in order to reduce the maintenance cost of the plugin that created the ledger. If a ledger is frozen, it can be used for reading but not for writing, and so would be easier to maintain.

#### How it affects the system

- Frozen ledgers do not participate in catch-up because the LEDGERS_FREEZE transaction is part of the config ledger, which is applied before additional ledgers.

- Frozen ledgers are not part of the freshness check, and they do not send new batches to update frozen ledgers.

- Root hashes of frozen ledgers continue to be recorded in new transactions on the audit ledger. These hashes are taken from the current config state, and not re-calculated from the real ledger (which may have been dropped).

- Any transactions to frozen ledgers are forbidden.
