import shutil
import sys
import os
from setuptools import setup, find_packages, __version__
from pip.req import parse_requirements

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


setup(
    name='plenum',
    version=__version__,
    description='Plenum Byzantine Fault Tolerant Protocol',
    long_description='Plenum Byzantine Fault Tolerant Protocol',
    url='https://github.com/evernym/plenum',
    download_url='https://github.com/evernym/plenum/tarball/{}'.
        format(__version__),
    author=__author__,
    author_email='dev@evernym.us',
    license=__license__,
    keywords='Byzantine plenum',
    packages=find_packages(exclude=['test', 'test.*',
                                    'docs', 'docs*']),
    package_data={
        '':       ['*.txt',  '*.md', '*.rst', '*.json', '*.conf', '*.html',
                   '*.css', '*.ico', '*.png', 'LICENSE', 'LEGAL']},
    install_requires=['raet', 'jsonpickle', 'portalocker', 'prompt_toolkit',
                      'pygments', 'ledger'],
    setup_requires=['pytest-runner'],
    tests_require=['pytest', 'pytest-xdist'],
    extras_require={
        'persistence': ['rethinkdb', 'motor']
    },
    scripts=['scripts/plenum', 'scripts/init_plenum_raet_keep', 'scripts/start_plenum_node']
)

BASE_DIR = os.path.join(os.path.expanduser("~"), ".plenum")
CONFIG_FILE = os.path.join(BASE_DIR, "plenum_config.py")
POOL_TXN_FILE = os.path.join(BASE_DIR, "pool_transactions")

if not os.path.exists(BASE_DIR):
    os.makedirs(BASE_DIR)

if not os.path.exists(CONFIG_FILE):
    with open(CONFIG_FILE, 'w') as f:
        msg = "# Here you can create config entries according to your needs.\n " \
              "# For help, refer config.py in the sovrin package.\n " \
              "# Any entry you add here would override that from config example\n"
        f.write(msg)

if not os.path.exists(POOL_TXN_FILE):
    shutil.copyfile("plenum/pool_transactions", POOL_TXN_FILE)
