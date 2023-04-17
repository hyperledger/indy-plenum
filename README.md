![logo](indy-logo.png)

[![Open in Gitpod](https://gitpod.io/button/open-in-gitpod.svg)](https://gitpod.io/#https://github.com/hyperledger/indy-plenum/tree/main)


- [Announcements](#announcements)
  - [April 12 2023](#april-12-2023)
- [Plenum Byzantine Fault Tolerant Protocol](#plenum-byzantine-fault-tolerant-protocol)
- [Technical Overview of Indy Plenum](#technical-overview-of-indy-plenum)
- [Other Documentation](#other-documentation)
- [Indy Plenum Repository Structure](#indy-plenum-repository-structure)
- [Dependencies](#dependencies)
- [Contact Us](#contact-us)
- [How to Contribute](#how-to-contribute)
- [How to Start Working with the Code](#how-to-start-working-with-the-code)

## Announcements

### April 12 2023

**_The project branches have changed._**

The `main` branch now contains the Ubuntu 20.04 work stream, and the previous `main` branch containing the Ubuntu 16.04 work stream has been moved to the `ubuntu-16.04` branch.  We encourage everyone to switch to using the new code and appreciate your patience while we stabilize the work flows and documentation on this new branch.

The following changes were made to the branches:
- `main` (default) renamed to `ubuntu-16.04`
  - This retargeted the associated PRs.
- `ubuntu-20.04-upgrade` set as the default branch.
- `ubuntu-20.04-upgrade` (default) renamed to `main`

## Plenum Byzantine Fault Tolerant Protocol

Plenum is the heart of the distributed ledger technology inside Hyperledger
Indy. As such, it provides features somewhat similar in scope to those
found in Fabric. However, it is special-purposed for use in an identity
system, whereas Fabric is general purpose.

## Technical Overview of Indy Plenum

Refer to our documentation site at [indy.readthedocs.io](https://hyperledger-indy.readthedocs.io/projects/plenum/en/latest/index.html) for the most current documentation and walkthroughs.

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

The preferred method of setting up the development environment is to use the devcontainers.
All configuration files for VSCode and [Gitpod](https://gitpod.io) are already placed in this repository.
If you are new to the concept of devcontainers in combination with VSCode [here](https://code.visualstudio.com/docs/remote/containers) is a good article about it.

Simply clone this repository and VSCode will most likely ask you to open it in the devcontainer, if you have the correct extension("ms-vscode-remote.remote-containers") installed.
If VSCode didn't ask to open it, open the command palette and use the `Remote-Containers: Rebuild and Reopen in Container` command.

If you want to use Gitpod simply use this [link](https://gitpod.io/#https://github.com/hyperledger/indy-plenum/tree/main)
or if you want to work with your fork, prefix the entire URL of your branch with  `gitpod.io/#` so that it looks like `https://gitpod.io/#https://github.com/hyperledger/indy-plenum/tree/main`.

**Note**: Be aware that the config files for Gitpod and VSCode are currently only used in the `main` branch!

Please have a look at [Dev Setup](https://github.com/hyperledger/indy-node/blob/master/docs/setup-dev.md) in indy-node repo.
It contains common setup for both indy-plenum and indy-node.



