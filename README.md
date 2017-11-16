# Plenum Byzantine Fault Tolerant Protocol    

Plenum is the heart of the distributed ledger technology inside Hyperledger
Indy. As such, it provides features somewhat similar in scope to those
found in Fabric. However, it is special-purposed for use in an identity
system, whereas Fabric is general purpose.

## Other Documentation

- Details about the protocol, including a great tutorial, can be found on the [wiki](https://github.com/hyperledger/indy-plenum/wiki).
- Please have a look at aggregated documentation at [indy-node-documentation](https://github.com/hyperledger/indy-node/blob/master/README.md) which describes workflows and setup scripts common for both projects. 

## Indy Plenum Repository Structure

- plenum:
    - the main codebase for plenum including Byzantine Fault Tolerant Protocol based on [RBFT](https://pakupaku.me/plaublin/rbft/5000a297.pdf)
- common:
    - common and utility code
- crypto:
    - basic crypto-related code (in particular, [indy-crypto](https://github.com/hyperledger/indy-crypto) wrappers) 
- ledger:
    - Provides a simple, python-based, immutable, ordered log of transactions 
backed by a merkle tree.
    - This is an efficient way to generate verifiable proofs of presence
and data consistency.
    - The scope of concerns here is fairly narrow; it is not a full-blown
distributed ledger technology like Fabric, but simply the persistence
mechanism that Plenum needs.
- state:
    - state storage using python 3 version of Ethereum's Patricia Trie
- stp:
    - secure transport abstraction
    - it has two implementations: RAET and ZeroMQ
    - Although RAET implementation is there, it's not supported anymore, and [ZeroMQ](http://zeromq.org/) is the default secure transport in plenum. 
- storage:
    - key-value storage abstractions
    - contains [leveldb](http://leveldb.org/) implementation as the main key-valued storage used in Plenum (for ledger, state, etc.)

## Dependencies

- Plenum makes extensive use of coroutines and the async/await keywords in
Python, and as such, requires Python version 3.5.0 or later. 
- Plenum also depends on [libsodium](https://download.libsodium.org/doc/), an awesome crypto library. These need to be installed
separately. 
- Plenum uses [ZeroMQ](http://zeromq.org/) as a secure transport
- [indy-crypto](https://github.com/hyperledger/indy-crypto)
    - A shared crypto library 
    - It's based on [AMCL](https://github.com/milagro-crypto/amcl)
    - In particular, it contains BLS multi-signature crypto needed for state proofs support in Indy.


## Contact us

- Bugs, stories, and backlog for this codebase are managed in [Hyperledger's Jira](https://jira.hyperledger.org).
Use project name `INDY`.
- Join us on [Jira's Rocket.Chat](https://chat.hyperledger.org/channel/indy) at `#indy` and/or `#indy-node` channels to discuss.

## How to Contribute

- We'd love your help; see these [instructions on how to contribute](http://bit.ly/2ugd0bq).
- You may also want to read this info about [maintainers](https://github.com/hyperledger/indy-node/blob/stable/MAINTAINERS.md).


## How to Start Working with the Code

Please have a look at [Dev Setup](https://github.com/hyperledger/indy-node/blob/master/docs/setup-dev.md) in indy-node repo.
It contains common setup for both indy-plenum and indy-node.


## Installing Plenum

#### Install from pypi


```
pip install indy-plenum
```

From here, you can play with the command-line interface (see the [tutorial](https://github.com/hyperledger/indy-plenum/wiki)).

Note: For Windows, we recommended using either [cmder](http://cmder.net/) or [conemu](https://conemu.github.io/).

```
plenum
```

...or run the tests.

```
git clone https://github.com/hyperledger/indy-plenum.git
cd indy-plenum
python -m plenum.test
```


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


#### Seeds used for generating clients
1. Seed used for steward Bob's signing key pair ```11111111111111111111111111111111```
2. Seed used for steward Bob's public private key pair ```33333333333333333333333333333333```
3. Seed used for client Alice's signing key pair ```22222222222222222222222222222222```
4. Seed used for client Alice's public private key pair ```44444444444444444444444444444444```


#### Running Node

```
start_plenum_node Alpha
```


#### Updating configuration
To update any configuration parameters, you need to update the `plenum_config.py` in `.plenum/YOUR_NETWORK_NAME` directory inside your home directory. 
eg. To update the node registry to use `127.0.0.1` as host put these in your `plenum_config.py`.

```python
from collections import OrderedDict

nodeReg = OrderedDict([
    ('Alpha', (('127.0.0.1', 9701), '0490a246940fa636235c664b8e767f2a79e48899324c607d73241e11e558bbd7', 'ea95ae1c913b59b7470443d79a6578c1b0d6e1cad0471d10cee783dbf9fda655')),
    ('Beta', (('127.0.0.1', 9703), 'b628de8ac1198031bd1dba3ab38077690ca9a65aa18aec615865578af309b3fb', '18833482f6625d9bc788310fe390d44dd268427003f9fd91534e7c382501cd3c')),
    ('Gamma', (('127.0.0.1', 9705), '92d820f5eb394cfaa8d6e462f14708ddecbd4dbe0a388fbc7b5da1d85ce1c25a', 'b7e161743144814552e90dc3e1c11d37ee5a488f9b669de9b8617c4af69d566c')),
    ('Delta', (('127.0.0.1', 9707), '3af81a541097e3e042cacbe8761c0f9e54326049e1ceda38017c95c432312f6f', '8b112025d525c47e9df81a6de2966e1b4ee1ac239766e769f19d831175a04264'))
])

cliNodeReg = OrderedDict([
    ('AlphaC', (('127.0.0.1', 9702), '0490a246940fa636235c664b8e767f2a79e48899324c607d73241e11e558bbd7', 'ea95ae1c913b59b7470443d79a6578c1b0d6e1cad0471d10cee783dbf9fda655')),
    ('BetaC', (('127.0.0.1', 9704), 'b628de8ac1198031bd1dba3ab38077690ca9a65aa18aec615865578af309b3fb', '18833482f6625d9bc788310fe390d44dd268427003f9fd91534e7c382501cd3c')),
    ('GammaC', (('127.0.0.1', 9706), '92d820f5eb394cfaa8d6e462f14708ddecbd4dbe0a388fbc7b5da1d85ce1c25a', 'b7e161743144814552e90dc3e1c11d37ee5a488f9b669de9b8617c4af69d566c')),
    ('DeltaC', (('127.0.0.1', 9708), '3af81a541097e3e042cacbe8761c0f9e54326049e1ceda38017c95c432312f6f', '8b112025d525c47e9df81a6de2966e1b4ee1ac239766e769f19d831175a04264'))
])
```
