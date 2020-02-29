
# Error handling

Basically errors can be split into two categories:
- expected errors
- unexpected errors

## Expected errors

These are errors that we expect to happen during normal operation and we must have a plan for dealing with it.
Examples of such errors are request validation errors or malformed or mistimed consesus messages.

Unfortunatelly there is no single concise strategy for handling such things over all codebase, in some cases we raise and catch exceptions, in other cases we basically branch on detecting things we don't consider normal.
However within specific parts of code (request handling, dynamic validation, processing consensus message etc) strategies are consistent.

### Client request validation

If it turns out that request is invalid we raise `InvalidClientRequest` exception, which is intercepted by generic request handling code and turned into REJECT or REQNACK reply to client.
Good example of using such exception can be found [here](https://github.com/hyperledger/indy-plenum/blob/master/plenum/server/request_handlers/txn_author_agreement_handler.py#L24) and [here](https://github.com/hyperledger/indy-plenum/blob/master/plenum/server/request_handlers/txn_author_agreement_handler.py#L85).

### Consensus messages validation

In most cases we separated validation code for consensus messages (like PREPREPARE, COMMIT, VIEW_CHANGE, etc) into separate functions which are called from message handlers. Processing logic depends on return values of such validator functions, which typically return a tuple of action (which can be process, discard or stash) and error string, which are passed to higher level generic processing code which can log that error when discarding message or can stash that message for a better time.
Good example of using this strategy can be found [here](https://github.com/hyperledger/indy-plenum/blob/master/plenum/server/consensus/view_change_service.py#L162) and [here](https://github.com/hyperledger/indy-plenum/blob/master/plenum/server/consensus/view_change_service.py#L223).

## Unexpected errors

These are errors that we don't expect to happen at all unless we've got some bugs.
It can be some violated preconditions, postconditions or invariants.
In this case we'd better just crash as soon as possible with informative error message and stack trace, especially when it happens during normal operation, not a startup. 
We prefer to do so in such situations because the sooner we stop the less are chances that some serious damage happens. 
Also `indy-node` service has autorestart logic in case of crashes so node would go back to operation state really quick.
These crashes are induced by raising `LogicError` exception (with message describing the problem) and deliberately not catching it.
In some way it is very similar to `assert`, with the difference that LogicError exceptions are raised in production builds as well, unlike asserts which are usually skipped.
With code which can be run during startup situation is a bit more tricky, because crashing at this point may lead to perpetual restart loop, rendering node effectively unavailable.
If we end up in such situation it is better to investigate problem more deeply (even if takes more time) and try to redesign a module in such a way that only expected errors are possible (not counting things like out of memory errors and totally unexpected bugs).

A bit more examples of error handling can be found [here](https://github.com/hyperledger/indy-plenum/blob/master/common/error_handling.py)