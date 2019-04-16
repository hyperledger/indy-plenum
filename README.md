![logo](indy-logo.png)

* [Plenum Byzantine Fault Tolerant Protocol](#plenum-byzantine-fault-tolerant-protocol)
* [Technical Overview of Indy Plenum](#technical-overview-of-indy-plenum)
* [Other Documentation](#other-documentation)
* [Indy Plenum Repository Structure](#indy-plenum-repository-structure)
* [Dependencies](#dependencies)
* [Contact Us](#contact-us)
* [How to Contribute](#how-to-contribute)
* [How to Start Working with the Code](#how-to-start-working-with-the-code)
* [Try Plenum Locally](#try-plenum-locally)

## Plenum Byzantine Fault Tolerant Protocol    

Plenum is the heart of the distributed ledger technology inside Hyperledger
Indy. As such, it provides features somewhat similar in scope to those
found in Fabric. However, it is special-purposed for use in an identity
system, whereas Fabric is general purpose.

## Technical Overview of Indy Plenum

Refer to our documentation site at [indy.readthedocs.io](https://indy.readthedocs.io/projects/plenum/en/latest/index.html) for the most current documentation and walkthroughs. 

Please find the general overview of the system in [Overview of the system](docs/source/main.md).

Plenum's consensus protocol which is based on [RBFT](https://pakupaku.me/plaublin/rbft/5000a297.pdf) is described in [consensus protocol diagram](docs/source/diagrams/consensus-protocol.png).

More documentation can be found in [docs](docs).

## Other Documentation

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
    - it has [ZeroMQ](http://zeromq.org/) implementations
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


## Contact Us

- Bugs, stories, and backlog for this codebase are managed in [Hyperledger's Jira](https://jira.hyperledger.org).
Use project name `INDY`.
- Join us on [Jira's Rocket.Chat](https://chat.hyperledger.org/channel/indy) at `#indy` and/or `#indy-node` channels to discuss.

## How to Contribute

- We'd love your help; see these [instructions on how to contribute](https://wiki.hyperledger.org/display/indy/How+to+Contribute).
- You may also want to read this info about [maintainers](https://github.com/hyperledger/indy-node/blob/stable/MAINTAINERS.md).


## How to Start Working with the Code

Please have a look at [Dev Setup](https://github.com/hyperledger/indy-node/blob/master/docs/setup-dev.md) in indy-node repo.
It contains common setup for both indy-plenum and indy-node.



