Glossary
========

   request
      a request sent by the client

   message
      any message passed between the nodes

   instance
      usually means the plen um protocol instance. When talking about "instance on the node" it means the replica of the protocol instance on the node

   election
      process to decide which replica in a protocol instance would be the primary

   view change
      process of choosing a new primary replica for each protocol instance, when
      the current master's throughput or other performance metrics are lower by
      certain thresholds than that of other protocol instances

   view
      a configuration that is reached when the primary of a protocol instance is changed

   request digest
      a hash of the request

