# Plenum Byzantine Fault Tolerant Protocol

**Details about the protocol, including a great tutorial, can be found on the [wiki](https://github.com/evernym/plenum/wiki).**

Plenum makes extensive use of coroutines and the async/await keywords in 
Python, 
and as such, requires Python version 3.5.0 or later and libsodium. 

## Installation Steps:

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

2. Open the downloaded file to install it.

3. If you are a homebrew fan, you can install it using this brew command: ```brew install python3``` 

4. To install homebrew package manager see: http://brew.sh/

5. Once you have the homebrew installed on your machine, run ```brew install libsodium``` to install libsodium. 


### Using a virtual environment(recommended but not necessary)
We recommend creating a new virtual environment for trying out Plenum. 
Virtual environment is a Python environment which is isolated from the 
system's default Python environment(you can change that) and any other 
virtual environment you create. You can create a new virtual environment with

```
virtaulenv -p python3.5 <name of virtual environment>
```

and now activate the virtual environment with

```
source <name of virtual environment>/bin/activate
```

### Installing the Plenum project

You can install Plenum using pip by

```
pip install plenum
```

Or you can clone this repo and then from the root directory of the repo, 
install it:

```
pip install -e .
```

From here on, you can play with the command-line interface:

```
plenum
```

or

From the repo

```
python -m plenum.cli
```

or

```
scripts/cli
```

Or you can run the tests:

```
python -m plenum.test
```