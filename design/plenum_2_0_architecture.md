# Plenum 2.0 Architecture

## General Principles

- Support micro-service architecture with a clear separation of concerns and possibility for efficient unit, integration and simulation testing.

- Plenum becomes multi-process application where each process may consist of multiple Services.

- Services do not communicate with each other directly, only via Buses (Observer pattern):

    - `InternalEventBus`: service-to-service communication within one Process on a Node
    - `ExternalEventBus`: node-to-node and client-to-node communication
    - `InterProcessEventBus`: service-to-service communication between multiple processes on a Node
    
- Services communicate via Messages:

    - Internal Messages (via `InternalEventBus` or `InterProcessEventBus`)
    - External Messages including 3PC messages (via `ExternalEventBus`)
    
- Each service explicitly subscribes to the events (messages) it's interested in.
    
## Main Services 

A list of processes and services run in each process:

- Read Request Process:
    - `ReadRequestService`: processes read request with a read-only access to the main ledger and state. Subscribes to:
        - all read (GET) requests
        - all action requests 

- Catchup Seeder Process: 
    - `CatchupNodeSeederService`: processes catchup requests from other  nodes. Accesses main ledger and state in a read-only mode. Subscribes to:
        - LEDGER_STATUS
        - CATCHUP_REQ  
    - `CatchupClientSeederService`: processes catchup requests from  clients. Accesses main ledger and state in a read-only mode. Subscribes to:
        - LEDGER_STATUS
        - CATCHUP_REQ     
    
- Node Process (Replicas Manager): 
   - `MonitorService`: coordinates view change between replicas (comparing performance of replicas)
    - `PropagateService`: propagates write requests to replicas. Subscribes to:
        - all write (transaction) requests
        - PROPAGATE
    
- Replica Process (a process for each replica). Replica processes don't communicate directly, only via Node Process.
   - `OrdererService`:
        - Three-Phase Commit logic.
        - Uses `WriteRequestManager` for validation, applying and committing requests. 
        - Needs write access to ledger and state.
        - Subscribes to:
            - PRE_PREPARE
            - PREPARE
            - COMMIT
    - `CheckpointerService`: Checkpoint and their stabilization logic. Subscribes to:
        - CHECKPOINT
    - `ViewChangerService`: View Change logic. Subscribes to:
        - VIEW_CHANGE
        - VIEW_CHANGE_ACK
        - NEW_VIEW
    - `CatchupLeecherService`: Performs catch-up of the given replica. Needs write access to ledger and state.  Subscribes to:
        - LEDGER_STATUS
        - CONSISTENCY_PROOF
        - CATCHUP_REP



## Network Interfaces
Each process has 1 or many network interfaces listening for incoming messages (either from clients or other nodes) and sending the messages (either to clients or other nodes):

   - __Client Read Requests Network Interface__: receiving read requests from clients and sending replies to them.
   - __Catchup Seeder Network Interface__: receiving catchup requests from clients and nodes and sending replies to them.
   - __Client Write Request Network Interface__: receiving write requests from clients and sending replies to them.
   - __Propagate Network Interface__: exchanging propagates between nodes.
   - __Replica K Network Interface, K=1.. F+1__: exchanging 3PC messages between replicas of the given protocol instances.

    
     