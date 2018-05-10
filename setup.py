import os
import sys

from setuptools import setup, find_packages

v = sys.version_info
if sys.version_info < (3, 5):
    msg = "FAIL: Requires Python 3.5 or later, " \
          "but setup.py was run using {}.{}.{}"
    v = sys.version_info
    print(msg.format(v.major, v.minor, v.micro))
    # noinspection PyPackageRequirements
    print("NOTE: Installation failed. Run setup.py using python3")
    sys.exit(1)

# Change to ioflo's source directory prior to running any command
try:
    SETUP_DIRNAME = os.path.dirname(__file__)
except NameError:
    # We're probably being frozen, and __file__ triggered this NameError
    # Work around this
    SETUP_DIRNAME = os.path.dirname(sys.argv[0])

if SETUP_DIRNAME != '':
    os.chdir(SETUP_DIRNAME)

SETUP_DIRNAME = os.path.abspath(SETUP_DIRNAME)

METADATA = os.path.join(SETUP_DIRNAME, 'plenum', '__metadata__.py')
# Load the metadata using exec() so we don't trigger an import of ioflo.__init__
exec(compile(open(METADATA).read(), METADATA, 'exec'))

tests_require = ['pytest==3.4.1', 'pytest-xdist==1.22.1', 'python3-indy==1.4.0-dev-509']

setup(
    name='indy-plenum-dev',
    version=__version__,
    description='Plenum Byzantine Fault Tolerant Protocol',
    long_description='Plenum Byzantine Fault Tolerant Protocol',
    url='https://github.com/hyperledger/indy-plenum',
    download_url='https://github.com/hyperledger/indy-plenum/tarball/{}'.
        format(__version__),
    author=__author__,
    author_email='hyperledger-indy@lists.hyperledger.org',
    license=__license__,
    keywords='Byzantine Fault Tolerant Plenum',
    packages=find_packages(exclude=['test', 'test.*', 'docs', 'docs*']) + [
        'data', ],
    package_data={
        '': ['*.txt', '*.md', '*.rst', '*.json', '*.conf', '*.html',
             '*.css', '*.ico', '*.png', 'LICENSE', 'LEGAL', 'plenum']},
    include_package_data=True,
    install_requires=['jsonpickle==0.9.6', 'ujson==1.33',
                      'prompt_toolkit==0.57', 'pygments==2.2.0',
                      'rlp==0.5.1', 'sha3==0.2.1', 'leveldb',
                      'ioflo==1.5.4', 'semver==2.7.9', 'base58==1.0.0', 'orderedset==2.0',
                      'sortedcontainers==1.5.7', 'psutil==5.4.3', 'pip<10.0.0',
                      'portalocker==0.5.7', 'pyzmq==17.0.0', 'libnacl==1.6.1',
                      'six==1.11.0', 'psutil==5.4.3', 'intervaltree==2.1.0',
                      'msgpack-python==0.4.6', 'indy-crypto==0.4.1',
                      'python-rocksdb==0.6.9', 'python-dateutil==2.6.1'],
    setup_requires=['pytest-runner'],
    extras_require={
        'tests': tests_require,
        'stats': ['python-firebase'],
        'benchmark': ['pympler']
    },
    tests_require=tests_require,
    scripts=['scripts/plenum', 'scripts/init_plenum_keys',
             'scripts/start_plenum_node',
             'scripts/generate_plenum_pool_transactions',
             'scripts/gen_steward_key', 'scripts/gen_node',
             'scripts/export-gen-txns', 'scripts/get_keys',
             'scripts/udp_sender', 'scripts/udp_receiver', 'scripts/filter_log',
             'scripts/log_stats',
             'scripts/init_bls_keys']
)
