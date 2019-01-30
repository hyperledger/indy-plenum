# Start Nodes

#### Notification
Please be aware that recommended way of starting a pool is to [use Docker](https://github.com/hyperledger/indy-node/blob/master/environment/docker/pool/README.md).



#### Initializing Keys
Each Node needs to have keys initialized
 - ed25519 transport keys (used by ZMQ for Node-to-Node and Node-to-Client communication)
 - BLS keys for BLS multi-signature and state proofs support
 
```
init_plenum_keys --name Alpha --seeds 000000000000000000000000000Alpha Alpha000000000000000000000000000 --force
```

```
init_plenum_keys --name Beta --seeds 0000000000000000000000000000Beta Beta0000000000000000000000000000 --force
```

```
init_plenum_keys --name Gamma --seeds 000000000000000000000000000Gamma Gamma000000000000000000000000000 --force
```

```
init_plenum_keys --name Delta --seeds 000000000000000000000000000Delta Delta000000000000000000000000000 --force
```
Note: Seed can be any randomly chosen 32 byte value. It does not have to be in the format `00..<name of the node>`.



#### Running Node

```
start_plenum_node Alpha
```

