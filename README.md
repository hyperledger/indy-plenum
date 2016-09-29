# Plenum Byzantine Fault Tolerant Protocol

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

3. Run ```sudo apt-get install python3.5```

4. First, check that the universe repository is enabled by inspecting ```/etc/apt/sources.list``` file with your favorite editor.

5. You will need to use sudo to ensure that you have permissions to edit the file. If universe is not included then modify the file so that it does include the following line:
```deb http://us.archive.ubuntu.com/ubuntu vivid main universe```

6. Run ```sudo apt-get update```

7. Run ```sudo apt-get install libsodium13```

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
To run a node you need to generate its keys. The keys are stored in on disk in files in a location called `keep`. 
The  following generates keys for 4 nodes named `Alpha`, `Beta`, `Gamma` and `Delta` in the keep. 
The keep for node `Alpha` is located at `~/.plenum/Alpha`. 
```
init_plenum_raet_keep --name Alpha [--seed 000000000000000000000000000Alpha] [--force]
```

```
init_plenum_raet_keep --name Beta [--seed 0000000000000000000000000000Beta] [--force]
```

```
init_plenum_raet_keep --name Gamma [--seed 000000000000000000000000000Gamma] [--force]
```

```
init_plenum_raet_keep --name Delta [--seed 000000000000000000000000000Delta] [--force]
```
Note: `seed` is optional. Seed can be any randomly chosen 32 byte value. It does not have to be in the format `00..<name of the node>`.
`force` is optional too. If you use this `--force` then the existing keys will be overwritten.
To see the public keys of the node with name say `Alpha`, use the command
```
get_keys Alpha
```


### Running Node

```
start_plenum_node Alpha 9601 9602
```
The node uses a separate UDP channels for communicating with nodes and clients. 
The first port number is for the node-to-node communication channel and the second is for node-to-client communication channel.


### Updating configuration
To update any configuration parameters, you need to update the `plenum_config.py` in `.plenum` directory inside your home directory. 
