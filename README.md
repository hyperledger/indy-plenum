# Plenum Byzantine Fault Tolerant Protocol    

Plenum is the heart of the distributed ledger technology inside Hyperledger
Indy. As such, it provides features somewhat similar in scope to those
found in Fabric. However, it is special-purposed for use in an identity
system, whereas Fabric is general purpose.

You can log bugs against Plenum in [Hyperledger's Jira](https://jira.hyperledger.org); use
project "INDY".

Plenum makes extensive use of coroutines and the async/await keywords in
Python, and as such, requires Python version 3.5.0 or later. Plenum also
depends on libsodium, an awesome crypto library. These need to be installed
separately. Read below to see how.

Plenum has other dependencies, including the impressive
[RAET](https://github.com/saltstack/raet) for secure reliable communication
over UDP, but this and other dependencies are installed automatically with
Plenum.

### Installing Plenum

```
pip install plenum
```

From here, you can play with the command-line interface (see the [tutorial](https://github.com/evernym/plenum/wiki))...

Note: For Windows, we recommended using either [cmder](http://cmder.net/) or [conemu](https://conemu.github.io/).

```
plenum
```

...or run the tests.

```
git clone https://github.com/evernym/plenum.git
cd plenum
python -m plenum.test
```

**Details about the protocol, including a great tutorial, can be found on the [wiki](https://github.com/evernym/plenum/wiki).**

### Installing python 3.5 and libsodium:

**Ubuntu:**

1. Run ```sudo add-apt-repository ppa:fkrull/deadsnakes```

2. Run ```sudo apt-get update```

3. On Ubuntu 14, run ```sudo apt-get install python3.5``` (python3.5 is pre-installed on most Ubuntu 16 systems; if not, do it there as well.)

4. We need to install libsodium with the package manager. This typically requires a package repo that's not active by default. Inspect ```/etc/apt/sources.list``` file with your favorite editor (using sudo). On ubuntu 16, you are looking for a line that says ```deb http://us.archive.ubuntu.com/ubuntu xenial main universe```. On ubuntu 14, look for or add: ```deb http://ppa.launchpad.net/chris-lea/libsodium/ubuntu trusty main``` and ```deb-src http://ppa.launchpad.net/chris-lea/libsodium/ubuntu trusty main```.

5. Run ```sudo apt-get update```. On ubuntu 14, if you get a GPG error about public key not available, run this command and then, after, retry apt-get update: ```sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys B9316A7BC7917B12```

6. Install libsodium; the version depends on your distro version. On Ubuntu 14, run ```sudo apt-get install libsodium13```; on Ubuntu 16, run ```sudo apt-get install libsodium18```

8. If you still get the error ```E: Unable to locate package libsodium13``` then add ```deb http://ppa.launchpad.net/chris-lea/libsodium/ubuntu trusty main``` and ```deb-src http://ppa.launchpad.net/chris-lea/libsodium/ubuntu trusty main``` to your ```/etc/apt/sources.list```. 
Now run ```sudo apt-get update``` and then ```sudo apt-get install libsodium13``` 

**CentOS/Redhat:**

1. Run ```sudo yum install python3.5```

2. Run ```sudo yum install libsodium-devel```


**Mac:**

1. Go to [python.org](https://www.python.org) and from the "Downloads" menu, download the Python 3.5.0 package (python-3.5.0-macosx10.6.pkg) or later.

2. Open the downloaded file to install it.

3. If you are a homebrew fan, you can install it using this brew command: ```brew install python3```

4. To install homebrew package manager, see: [brew.sh](http://brew.sh/)

5. Once you have homebrew installed, run ```brew install libsodium``` to install libsodium.


**Windows:**

1. Go to https://download.libsodium.org/libsodium/releases/ and download the latest libsodium package (libsodium-1.0.8-mingw.tar.gz is the latest version as of this writing)

2. When you extract the contents of the downloaded tar file, you will see 2 folders with the names libsodium-win32 and libsodium-win64.

3. As the name suggests, use the libsodium-win32 if you are using 32-bit machine or libsodium-win64 if you are using a 64-bit operating system.

4. Copy the libsodium-x.dll from libsodium-win32\bin or libsodium-win64\bin to C:\Windows\System or System32 and rename it to libsodium.dll.

5. Download the latest build (pywin32-220.win-amd64-py3.5.exe is the latest build as of this writing) from  [here](https://sourceforge.net/projects/pywin32/files/pywin32/Build%20220/) and run the downloaded executable.


### Using a virtual environment (recommended)
We recommend creating a new Python virtual environment for trying out Plenum.
a virtual environment is a Python environment which is isolated from the
system's default Python environment (you can change that) and any other
virtual environment you create. You can create a new virtual environment by:
```
virtualenv -p python3.5 <name of virtual environment>
```

And activate it by:

```
source <name of virtual environment>/bin/activate
```


### Initializing Keep
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


### Seeds used for generating clients
1. Seed used for steward Bob's signing key pair ```11111111111111111111111111111111```
2. Seed used for steward Bob's public private key pair ```33333333333333333333333333333333```
3. Seed used for client Alice's signing key pair ```22222222222222222222222222222222```
4. Seed used for client Alice's public private key pair ```44444444444444444444444444444444```


### Running Node

```
start_plenum_node Alpha
```


### Updating configuration
To update any configuration parameters, you need to update the `plenum_config.py` in `.plenum` directory inside your home directory. 
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

# Immutable Ledger used in Plenum. 

This codebase provides a simple, python-based, immutable, ordered log of transactions 
backed by a merkle tree. This is an efficient way to generate verifiable proofs of presence
and data consistency.

The scope of concerns here is fairly narrow; it is not a full-blown
distributed ledger technology like Fabric, but simply the persistence
mechanism that Plenum needs. The repo is intended to be collapsed into the indy-node codebase
over time; hence there is no wiki, no documentation, and no intention to
use github issues to track bugs.

You can log issues against this codebase in [Hyperledger's Jira](https://jira.hyperledger.org).

Join us on [Hyperledger's Rocket.Chat](http://chat.hyperledger.org), on the #indy
channel, to discuss.

# state
Plenum's state storage using python 3 version of Ethereum's Patricia Trie

# stp
Secure Transport Protocol
