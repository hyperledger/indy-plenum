import os
import sys

from setuptools import setup, find_packages, __version__

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

BASE_DIR = os.path.join(os.path.expanduser("~"), ".plenum")
CONFIG_FILE = os.path.join(BASE_DIR, "plenum_config.py")
POOL_TXN_FILE = os.path.join(BASE_DIR, "pool_transactions_sandbox")

if not os.path.exists(BASE_DIR):
    os.makedirs(BASE_DIR)

setup(
    name='indy-plenum-dev',
    version=__version__,
    description='Plenum Byzantine Fault Tolerant Protocol',
    long_description='Plenum Byzantine Fault Tolerant Protocol',
    url='https://github.com/hyperledger/indy-plenum',
    download_url='https://github.com/hyperledger/indy-plenum/tarball/{}'.
        format(__version__),
    author=__author__,
    author_email='dev@evernym.us',
    license=__license__,
    keywords='Byzantine Fault Tolerant Plenum',
    packages=find_packages(exclude=['test', 'test.*', 'docs', 'docs*']) + [
        'data', ],
    package_data={
        '': ['*.txt', '*.md', '*.rst', '*.json', '*.conf', '*.html',
             '*.css', '*.ico', '*.png', 'LICENSE', 'LEGAL', 'plenum']},
    include_package_data=True,
    data_files=[(
        (BASE_DIR, ['data/pool_transactions_sandbox', ])
    )],
    install_requires=['jsonpickle', 'ujson==1.33',
                      'prompt_toolkit==0.57', 'pygments',
                      'crypto==1.4.1', 'rlp', 'sha3', 'leveldb',
                      'ioflo==1.5.4', 'semver', 'base58', 'orderedset',
                      'sortedcontainers==1.5.7', 'psutil', 'pip',
                      'portalocker==0.5.7', 'pyzmq', 'raet',
                      'psutil', 'intervaltree'],
    extras_require={
        'stats': ['python-firebase'],
        'benchmark': ['pympler']
    },
    setup_requires=['pytest-runner'],
    tests_require=['pytest', 'pytest-xdist'],
    scripts=['scripts/plenum', 'scripts/init_plenum_keys',
             'scripts/start_plenum_node',
             'scripts/generate_plenum_pool_transactions',
             'scripts/gen_steward_key', 'scripts/gen_node',
             'scripts/export-gen-txns', 'scripts/get_keys',
             'scripts/udp_sender', 'scripts/udp_receiver']
)

if not os.path.exists(CONFIG_FILE):
    with open(CONFIG_FILE, 'w') as f:
        msg = "# Here you can create config entries according to your " \
              "needs.\n " \
              "# For help, refer config.py in the sovrin package.\n " \
              "# Any entry you add here would override that from config " \
              "example\n"
        f.write(msg)


# TODO: This code should not be copied here.
import getpass
import os
import shutil
import sys


def getLoggedInUser():
    if sys.platform == 'wind32':
        return getpass.getuser()
    else:
        if 'SUDO_USER' in os.environ:
            return os.environ['SUDO_USER']
        else:
            return getpass.getuser()


def changeOwnerAndGrpToLoggedInUser(directory, raiseEx=False):
    loggedInUser = getLoggedInUser()
    try:
        shutil.chown(directory, loggedInUser, loggedInUser)
    except Exception as e:
        if raiseEx:
            raise e
        else:
            pass


changeOwnerAndGrpToLoggedInUser(BASE_DIR)

