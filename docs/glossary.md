Glossary
========
   
   Pool: The collection of nodes running the consensus protocol 
      
   Instance: Usually means the plenum protocol instance. When talking about "instance on the node" it means the replica of the protocol instance on the node
    
   View: A configuration of the pool with designated leaders for each instance. Each view is known by a unique integer.  

   View change: Process of choosing a new primary replica for each protocol instance, when 
   the current master's throughput or other performance metrics are lower by 
   certain thresholds than that of other protocol instances
    
   Election: Process to decide which replica in a protocol instance would be the primary
   
   Request: A message sent by the client to append into or query the ledger
   
   Request Digest: A sha256 hash of the serialised request data
