# Plenum Byzantine Fault Tolerant Protocol

### Installing Plenum

```
pip install plenum
```

From here, you can play with the command-line interface (see the [tutorial](https://github.com/evernym/plenum/wiki))...

```
plenum
```

...or run the tests.

```
python -m plenum.test
```

**Details about the protocol, including a great tutorial, can be found on the [wiki](https://github.com/evernym/plenum/wiki).**


## Installation Steps:

Plenum makes extensive use of coroutines and the async/await keywords in 
Python, and as such, requires Python version 3.5.0 or later. 

Plenum also depends on libsodium, an awesome crypto library.

Plenum has other dependencies, including the impressive [RAET](https://github.com/saltstack/raet) for secure reliable communication over UDP.

### Install python 3.5 and libsodium:

**Ubuntu:**

1. Run ```sudo add-apt-repository ppa:fkrull/deadsnakes```

2. Run ```sudo apt-get update```

3. Run ```sudo apt-get install python3.5```

4. First, check that the universe repository is enabled by inspecting ```/etc/apt/sources.list``` file with your favorite editor.

5. You will need to use sudo to ensure that you have permissions to edit the file. If universe is not included then modify the file so that it does include the following line:
```deb http://us.archive.ubuntu.com/ubuntu vivid main universe```

6. Run ```sudo apt-get update```

7. Run ```sudo apt-get install libsodium13```

**CentOS/Redhat:**

1. Run ```sudo yum install python3.5```

2. Run ```sudo yum install libsodium-devel```

**Mac:**

1. Go to https://www.python.org and from the "Downloads" menu, download the Python 3.5.1 package(python-3.5.1-macosx10.6.pkg).

1. Open the downloaded file to install it.

1. If you are a homebrew fan, you can install it using this brew command: ```brew install python3``` 

1. To install homebrew package manager, see: http://brew.sh/

1. Once you have homebrew installed, run ```brew install libsodium``` to install libsodium. 


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
