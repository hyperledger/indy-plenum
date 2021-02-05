#VM 20.04 Setup

##Pre-Install

        sudo apt-get update && sudo apt-get install -y apt-transport-https ca-certificates
        sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys CE7709D068DB5E88 || sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys CE7709D068DB5E88
        sudo echo "deb https://repo.sovrin.org/deb bionic master" >> /etc/apt/sources.list
        sudo echo "deb https://repo.sovrin.org/deb bionic stable" >> /etc/apt/sources.list
        sudo echo "deb http://security.ubuntu.com/ubuntu bionic-security main" >> /etc/apt/sources.list
        sudo apt-get update
        sudo apt-get install -y git wget unzip python3-pip python3-venv libsodium23 iptables at supervisor python3-nacl rocksdb-tools librocksdb5.17 librocksdb-dev libsnappy-dev liblz4-dev libbz2-dev libssl1.0.0 libindy ursa
        git clone https://github.com/hyperledger/indy-node.git
        git clone https://github.com/hyperledger/indy-plenum.git
        git checkout origin/ubuntu-20.04-upgrade in both indy-node and indy-plenum
        sudo cp /usr/lib/ursa/libursa.* /usr/lib/
        # Should be done in python env
        pip3 install -U wheel==0.34.2 python-rocksdb==0.6.9 python3-indy==1.13.0-dev-1420

##IDE Setup
    Pycharm: 
        # Open indy-node
        # Open indy-plenum - Link
        # Create virtual env in project structure - python interpreter
        # Create virtual env in project structure - python interpreter
        # All pip3 commands mentioned above must be done in env

## Base Dependencies Needed to test
### Library Dependencies:

###  Pip Dependencies:

    Pygments                2.2.0
    Pympler                 0.8  
    apipkg                  1.5  
    attrs                   19.1.0
    base58                  2.1.0
    execnet                 1.8.0
    iniconfig               1.1.1
    intervaltree            2.1.0
    ioflo                   1.5.4   
    jsonpickle              0.9.6
    leveldb                 0.201  
    libnacl                 1.6.1 
    msgpack-python          0.4.6
    orderedset              2.0.3   
    packaging               19.0   
    pip                     9.0.3
    pluggy                  0.6.0    
    portalocker             0.5.7    
    prompt-toolkit          0.57    
    psutil                  5.6.6    
    py                      1.10.0    
    pyparsing               2.4.7    
    pytest                  3.3.1    
    pytest-asyncio          0.8.0    
    pytest-forked           0.2    
    pytest-runner           5.2   
    pytest-xdist            1.22.1    
    python-dateutil         2.6.1    
    python-rocksdb          0.6.9    
    python-ursa             0.1.1    
    python3-indy            1.13.0    
    pyzmq                   18.1.0    
    rlp                     0.5.1    
    semver                  2.7.9    
    setuptools              53.0.0
    sha3                    0.2.1    
    six                     1.11.0   
    sortedcontainers        1.5.7    
    toml                    0.10.2    
    ujson                   1.33    
    wcwidth                 0.2.5    
    wheel                   0.34.2    

## Test Change:
    - indy/indy-plenum/stp_core/loop/looper.py

## Failing Tests
    - indy-plenum/plenum/test/freshness/test_replica_freshness.py
    - indy-plenum/plenum/test/metrics/test_metrics_collector.py

