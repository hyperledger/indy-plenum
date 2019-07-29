# Request handling

To handler requests sent by client, the node has a several managers:
* `write_manager`
* `read_manager`
* `action_manager`
All of this managers have a 2 type of handlers:
* `request handlers` (`WriteRequestHandler`, `ReadRequestHandler` and `ActionRequestHandler`)
* `batch handlers` (`Pool/Domain/Config/Audit BatchHandler`)
Request handler needs to making static and dynamic validation and updating state. Batch handlers perform a batch-related functions, like `apply_batch`, `commit_batch` and `reject_batch`.
All of managers have an API method for registering request and batch handlers, that's called `register_req_handler` and `register_batch_handler` correspondingly.
During static or dynamic validation all of handlers which associated with required transaction type will be called and performed. It means, that we can divide some specific validations into different request handlers.
Also, this logic is suitable for batch's handlers too. 

  
There are 3 types of requests a client can send:
-   Query:
    Here the client is requesting transactions or some state variables from the node(s). The client can either send a `GET_TXN` to get any transaction with a sequence number from any ledger.
    Or it can send specific `GET_*` transactions for queries. `read_manager` will be used for this request type.
-   Write:
    Here the client is asking the nodes to write a new transaction to the ledger and change some state variable. This requires the nodes to run a consensus protocol (currently RBFT).
    If the protocol run is successful, then the client's proposed changes are written. `write_manager` will be used for this request type.


#### Request handling
Below is a description of how a request is processed.
A node on receiving a client request in  `validateClientMsg`: 
-   The node performs static validation checks (validation that does not require any state, like mandatory fields are present, etc), it uses `ClientMessageValidator` and 
    `static_validation` from associated manager.
-   If the static validation passes, it checks if the signature check is required (not required for queries) and does that if needed in `verifySignature`. More on this later.
-   Checks if it's a generic transaction query (`GET_TXN`). If it is then query the ledger for that particular sequence number and return the result. A `REQNACK` might be sent if the query is not correctly constructed. 
-   Checks if it's a specific query, then corresponded `request_handler` from `read_manager` return a result. A `REQNACK` might be sent if the query is not correctly constructed.
-   If it is a write, then node checks if it has already processed the request before by checking the uniqueness of `identifier` and `reqId` fields of the Request.
    -   If it has already processed the request, then it sends the corresponding `Reply` to the client
    -   If the `Request` is already in process, then it sends an acknowledgement to the client as a `REQACK`
    -   If the node has not seen the `Request` before it broadcasts the `Request` to all nodes in a `PROPAGATE`.
    -   Once a node receives sufficient (`Quorums.propagate`) `PROPAGATE`s for a request, it forwards the request to its replicas.
    -   A primary replica on receiving a forwarded request does dynamic validation (requiring state, like if violating some unique constraint or doing an unauthorised action) 
        on the request calling `dynamic_validation` of `write_manager` which choose a specific request handlers. If the validation succeeds then 
        `apply_request` method of `write_manager` is called which optimistically applies the changes to ledger and state. 
        If the validation fails, the primary sends a `REJECT` to the client. Then the primary sends a `PRE-PREPARE` to other nodes which contains the merkle roots and some other information for all such requests.
    -   The non-primary replicas on receiving the `PRE-PREPARE` performs the same dynamic validation that the primary performed on each request. It also checks if the merkle roots match.
        If all checks pass the replicas send a `PREPARE` otherwise they reject the `PRE-PREPARE`
    -   Once the consensus protocol is successfully executed on the request, the replicas send `ORDERED` message to its node and the node updates the monitor.
    -   If the `ORDERED` message above was sent by the master replica then the node executes the request; meaning they commit any changes made to the ledger and state by that
        request by calling `commit_batch` method of `write_manager` and send a `Reply` back to client.
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
- WriteRequestManager: `plenum/server/request_managers/write_request_manager.py`
- ReadRequestManager: `plenum/server/request_managers/read_request_manager.py`
- ActionRequestManager: `plenum/server/request_managers/action_request_manager.py`
- WriteRequestHandler: `plenum/server/request_handlers/handler_interfaces/write_request_handler.py`
- ReadRequestHandler: `plenum/server/request_handlers/handler_interfaces/read_request_handler.py`
- ActionRequestHandler: `plenum/server/request_handlers/handler_interfaces/action_request_handler.py`
- Base RequestHandlers: `plenum/server/request_handlers/`
- Request Authenticator: `plenum/server/req_authenticator.py`
- Core Authenticator: `plenum/server/client_authn.py`
- Quorums: `plenum/server/quorums.py`
