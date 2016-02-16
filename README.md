# Zeno Byzantine Fault Tolerant Protocol

**Details about the protocol, including a great tutorial, can be found on the [wiki](https://github.com/evernym/zeno/wiki).**

Zeno makes extensive use of coroutines and the async/await keywords in Python, 
and as such, requires Python version 3.5.0 or later and libsodium. See the "Installation Steps" section for help.   
 
After cloning the repo, from the root directory of the repo, install it:

```
pip install -e .
```

From here, you can play with the command-line interface:

```
python -m zeno.cli
```

or

```
scripts/cli
```

Or you can run the tests:

```
python -m zeno.test
```

**Installation Steps:**

*Ubuntu:*

1. Run ```sudo add-apt-repository ppa:fkrull/deadsnakes```

2. Run ```sudo apt-get update```

3. Run ```sudo apt-get install python3.5```

4. First, check that the universe repository is enabled by inspecting ```/etc/apt/sources.list``` file with your favorite editor.

5. You will need to use sudo to ensure that you have permissions to edit the file. If universe is not included then modify the file so that it does include the following line:
```deb http://us.archive.ubuntu.com/ubuntu vivid main universe```

6. Run ```sudo apt-get update```

7. Run ```sudo apt-get install libsodium13```

*CentOS/Redhat:*

1. Run ```sudo yum install python3.5```

2. Run ```sudo yum install libsodium-devel```

*Mac:*

1. Go to https://www.python.org and from the "Downloads" menu, download the Python 3.5.1 package(python-3.5.1-macosx10.6.pkg).

2. Open the downloaded file to install it.

3. If you are homebrew fan, you can install it using this brew command: ```brew install python3``` 

4. To homebrew package manager see: http://brew.sh/

5. Once you have the homebrew installed on your machine, run ```brew install libsodium``` to install libsodium. 
