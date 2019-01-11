# Glossary

   -    Node: A physical machine that is running one replica per protocol instance. The nodes are known to each other.
      
   -    Instance: A logical construct that spans across all nodes, consisting of a replica from every node. Only one instance is the `Master` at a time. The rest are all called `Backup`
   
   -    Replica: A single node's representative to an instance. A replica lives at the intersection of a node and a protocol instance. It lives on one node, and is dedicated to one and only one instance.
   
   -    Pool: The collection of nodes running the consensus protocol 
    
   -    View: A configuration of the pool with designated leaders for each instance. Each view is known by a unique integer.  

   -    View change: Process of choosing a new primary replica for each protocol instance, when 
        the current master's throughput or other performance metrics are lower by 
        certain thresholds than that of other protocol instances
    
   -    Election: Process to decide which replica in a protocol instance would be the primary
   
   -    Request: A message sent by the client to write to or query from the ledger. Each request has a field called `operation` which is the payload of the request. The `operation` contains a field `type` which indicates the intent of the request.
   
   -    Request Digest: A sha256 hash of the serialised request data.
   
   -    Request ordering: Successful completion of consensus process over a request.
   
   -    Transaction: A write request successfully processed (ordered) by the pool. Each transaction has a field called `type` which indicates the intent of the transaction.
   
   -    Ledger: An ordered log of transactions. Each node hosts several ledgers which serve different purposes. Each ledger is uniquely identified by a ledger id. Each correct node hosts the same ledger as any other correct node. The ledger assigns each transaction a unique monotonically increasing positive integer called sequence number.
   
   -    State: A projection of the ledger. Exposes a key-value store like API, can provide merkle proof of presence of keys with values. Currently uses a Merkle Patricia Trie under the hood. 
   
   -    Catchup: The process of a node syncing a ledger with other nodes. Used when a node starts or during a view change or it finds itself lagging behind others.
   
   -    Ledger Manager: Contains logic for catchup. Allows registering ledgers to be synced and callbacks to be called at different events.
   
   -    Request Handler: A class containing the processing logic of requests of certain `type`. For write requests, updates the ledger and might update state too. 
   
   -    Client: Generic logic for sending requests to node(s), handling retries, acknowledgments, replies, etc. Allows registering callbacks that are called once a request has been successfully processed by the pool. 
   
   -    Wallet: Stores private keys and other secrets. Also contains code to act on successfully processed requests.
