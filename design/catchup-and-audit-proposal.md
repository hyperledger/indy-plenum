# Catchup and audit design proposal

## Why

### Requirement 1: No synchronization issues between any ledgers

**Problem:** When multiple ledgers have interdependencies current independent catch-up
of different ledgers can break things as follows:
- suppose we have ledgers A and B, and validation of transactions from A is dependent
  on contents of B
- suppose some node X started catch-up and caught up ledger A
- after that pool ordered transaction A1, then B1
- B1 would prohibit processing of A1 if B1 was ordered first
- note that node X doesn't contain A1 at this point, since catch up of ledger A was
  done before transaction A1 was ordered by pool
- node X continues catch-up and catches up ledger B already containing B1
- node X finishes catch up and tries to process stashed 3PC messages
- it learns that it should process A1, however it already has B1 which cause
  A1 to be rejected by node X
- so node X falls out of consensus

So, ledgers should be caught up consistently, that is, following previous example,
if A1 wasn't caught up then B1 shouldn't be caught up as well.

### Requirement 2: Auditability/history

It should be possible to audit if any given transaction was written to the ledger
legitimately, that is all validations really passed.

Examples:
- Validate authorization rules
- Domain-config: multi-signature values
- Pool-config: multi-signature values
- Pool-domain: whether Steward(s) have rights to add/edit NODE txn

The issue is caused by a fact, that  there is no way to restore order of transactions
between ledgers after they are written, and hence no way to know what was the total state
of the system at the moment of applying any given transaction to be able to audit if it
was applied correctly.

**Nice to have:** It should be _easily_ possible to audit if any given transaction was
written to the ledger legitimately. In other words, it should be easily possible to get
the current state of the system at the moment before the txn was applied.

Input:
- Transaction
- Ledger(s)

Output:
- Results of post-write audit (off-ledger checks for validation against the state
  at the moment when txn was written)

### Requirement 3 (optional): Deterministically and consistently restore 3PC state (last ordered batch viewNo/ppSeqNo)

Currently when enough nodes (but not all) restart simultaneously nodes that didn’t
restart fall out of consensus. Some sequence of restarts may lead to full loss of
consensus in pool. We have some partial fixes for this problem, but they are not
reliable and edge cases still exist.

## How: Add common ledger with links to other ledgers

### Description

Create one more ledger (let’s call it Audit), and for each ordered batch make
a record in it containing:
- `viewNo` and `ppSeqNo` of last ordered batch
- dictionary with key `ledger_id` and value `seq_no`
  - (Optional) Value can also include ledger and state root hash after applying batch

Also each PREPREPARE should include Audit ledger root hash, so that we can be sure
that Audit ledger is equal across all valid nodes.

No change in existing ledgers or transactions is required since with this new
Audit ledger it’s possible to unambiguously restore order of transactions processing
and validate state root hashes on per 3PC batch basis.

### Pros

- relatively simple to implement and maintain
- doesn't require migration
- fulfills all requirements (including optional ones)

### Cons

- adds one more ledger

### Catch-up

Catch-up can be done as follows:
- `LEDGER_STATUS` is asked for Audit ledger only, returning its last `seq_no`
  (let’s call it `audit_seq_no`)
- after figuring out that catch-up is needed node sends `REQUEST_CONSISTENCY_PROOF`
  consisting of:
  - `ledger_id`
  - minimum/maximum `audit_seq_no`
- node that receives `REQUEST_CONSISTENCY_PROOF`:
  - given min/max `audit_seq_no` finds corresponding min/max `seq_no` inside ledger
    defined by `ledger_id` (thanks to Audit ledger it’s O(1) operation)
  - sends `CONSISTENCY_PROOF` message as usual
- then catch-up proceeds as usual

These changes in first part of catch up make impossible situations when some ledger
catches up too many transactions.

### Audit

Audit can be performed as follows:
- auditor somehow obtains ledgers (including Audit ledger) from pool, either through
  catch-up or by directly downloading from some steward
- auditor starts building state by replaying transactions according to their order in
  Audit ledger
- during application of each transaction it’s possible to:
  - validate transaction against state to check whether its presence
    in ledger is legitimate
  - for each batch compare resulting state root hash with one from Audit ledger, so
    correctness of state can also be verified with per-batch granularity
- after initial work of restoring state from ledger is done additional ledger updates
  can be applied and audited incrementally (just continue replaying new batches on top
  of previous ones)
- during initial traversal it’s possible to build index for fast mapping
  `(ledger_id, seq_no) -> (audit_seq_no, batch_seq_no, is_valid, state_root)`,
  so given any transaction it’s straightforward to check whether it is really present
  in ledger, whether its presence is legitimate and whether it was applied correctly

### Restore 3PC state

Since last item in Audit ledger always contains last ordered 3PC batch, consistent
between nodes and can be synchronized using normal catch-up if needed it becomes
straightforward to restore last 3PC state.

### Optional improvement

Transaction metadata can also include state root hash before applying transaction.
This enables following improvements:
- legitimacy of transaction can be checked against state (obtained either by initial
  traversal of ledger or by direct download from steward) without building additional
  indexes
- during initial traversal it becomes possible to verify correctness of state on
  per-transaction granularity (base proposal offers only per-batch granularity)

### Interaction with freshness issue

Current PoA for dealing with freshness issue boils down to following:
- if some state doesn't get updated for long enough time nodes order empty batch
- so state root hash gets updated timestamp and signatures
- however ledgers don't grow

Adding these empty batches to audit ledger is not needed to fulfill first two
requirements, however adding them is required in order to fulfill third optional
requirement (restore 3PC state). Nevertheless, this doesn't seem as a big problem:
- expected size of entry in audit ledger is around 25 bytes (with 4 normal ledgers
  and MsgPack encoding)
- even if state gets updated every 1 minute there will be around 500k such updates
  per year
- which gives a little bit over 12 Mb overhead per year if pool doesn't order anything
- and if state gets updated every 5 minutes this overhead becomes just 2.5 Mb per year
