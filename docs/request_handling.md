# Request handling

To handle requests sent by client, the nodes require a ledger and/or a state as a data store. Request handling logic is written in `RequestHandler`. 
`RequestHandler` is extended by new classes to support new requests. A node provides methods `register_new_ledger` to register new `Ledger` and `register_req_handler` to register new `RequestHandler`s.
There can be a callback registered to execute after a write request has been successfully executed using `register_executer`.

  
There are 2 types of requests a client can send:
-   Query:
    Here the client is requesting transactions or some state variables from the node(s). The client can either send a `GET_TXN` to get any transaction with a sequence number from any ledger.
    Or it can send specific `GET_*` transactions for queries
-   Write:
    Here the client is asking the nodes to write a new transaction to the ledger and change some state variable. This requires the nodes to run a consensus protocol (currently RBFT).
    If the protocol run is successful, then the client's proposed changes are written. 


#### Request handling
Below is a description of how a request is processed.
A node on receiving a client request in  `validateClientMsg`: 
-   The node performs static validation checks (validation that does not require any state, like mandatory fields are present, etc), it uses `ClientMessageValidator` and 
    `RequestHandler`'s `doStaticValidation` for this.
-   If the static validation passes, it checks if the signature check is required (not required for queries) and does that if needed in `verifySignature`. More on this later.
-   Checks if it's a generic transaction query (`GET_TXN`). If it is then query the ledger for that particular sequence number and return the result. A `REQNACK` might be sent if the query is not correctly constructed. 
-   Checks if it's a specific query (needs a specific `RequestHandler`), if it is then it uses `process_query` that uses the specific `RequestHandler` to respond to the query. A `REQNACK` might be sent if the query is not correctly constructed.
-   If it is a write, then node checks if it has already processed the request before by checking the uniqueness of `identifier` and `reqId` fields of the Request.
    -   If it has already processed the request, then it sends the corresponding `Reply` to the client
    -   If the `Request` is already in process, then it sends an acknowledgement to the client as a `REQACK`
    -   If the node has not seen the `Request` before it broadcasts the `Request` to all nodes in a `PROPAGATE`.
    -   Once a node receives sufficient (`Quorums.propagate`) `PROPAGATE`s for a request, it forwards the request to its replicas.
    -   A primary replica on receiving a forwarded request does dynamic validation (requiring state, like if violating some unique constraint or doing an unauthorised action) 
        on the request using `doDynamicValidation` which in turn calls the corresponding `RequestHandler`'s `validate` method. If the validation succeeds then 
        `apply` method of `RequestHandler` is called which optimistically applies the changes to ledger and state. 
        If the validation fails, the primary sends a `REJECT` to the client. Then the primary sends a `PRE-PREPARE` to other nodes which contains the merkle roots and some other information for all such requests.
    -   The non-primary replicas on receiving the `PRE-PREPARE` performs the same dynamic validation that the primary performed on each request. It also checks if the merkle roots match.
        If all checks pass the replicas send a `PREPARE` otherwise they reject the `PRE-PREPARE`
    -   Once the consensus protocol is successfully executed on the request, the replicas send `ORDERED` message to its node and the node updates the monitor.
    -   If the `ORDERED` message above was sent by the master replica then the node executes the request; meaning they commit any changes made to the ledger and state by that
        request by calling the corresponding `RequestHandler`'s `commit` method and send a `Reply` back to client.
    -   The node also tracks the request's `identifier` and `reqId` in a key value database in `updateSeqNoMap`.


#### Signature verification
Each node has a `ReqAuthenticator` object which allows to register `ClientAuthNr` objects using `register_authenticator` method. During signature verification, 
a node runs each registered authenticator over the request and if any authenticator results in an exception then signature verification is considered failed.
A node has atleast 1 authenticator called `CoreAuthNr` whose `authenticate` method is called over the serialised request data to verify signature.


Relevant code:
- Node: `plenum/server/node.py`
- Replica: `plenum/server/replica.py`
- Propagator: `plenum/server/propagator.py`
- Request: `plenum/common/request.py`
- Request structure validation: `plenum/common/messages/client_request.py`
- RequestHandler: `plenum/server/req_handler.py`
- Request Authenticator: `plenum/server/req_authenticator.py`
- Core Authenticator: `plenum/server/client_authn.py`
- Quorums: `plenum/server/quorums.py`
