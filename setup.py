"""
setup.py

Basic setup file to enable pip install
See:
    https://pythonhosted.org/setuptools/
    https://bitbucket.org/pypa/setuptools


python setup.py register sdist upload

"""
import sys
import os
from setuptools import setup, find_packages, __version__

v = sys.version_info
if sys.version_info < (3, 5):
    msg = "FAIL: Requires Python 3.5 or later, " \
          "but setup.py was run using {}.{}.{}"
    v = sys.version_info
    print(msg.format(v.major, v.minor, v.micro))
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

REQ = {'SERVER': ['raet'],
       'COMMON': ['jsonpickle'],
       'CLI': ['prompt_toolkit', 'pygments'],
       'TEST': ['pytest']}
REQUIRES = set(sum(REQ.values(), []))
EXTRAS = {}

setup(
    name='plenum',
    version=__version__,
    description='Asynchronous Byzantine Agreement',
    long_description='Asynchronous Byzantine Agreement',
    url='https://bitbucket.org/evernym/plenum.git',
    download_url='https://bitbucket.org/evernym/plenum.git',
    author=__author__,
    author_email='',
    license=__license__,
    keywords='Byzantine',
    packages=find_packages(exclude=['test', 'test.*',
                                    'docs', 'docs*']),
    package_data={
        '':       ['*.txt',  '*.md', '*.rst', '*.json', '*.conf', '*.html',
                   '*.css', '*.ico', '*.png', 'LICENSE', 'LEGAL']},
    install_requires=REQUIRES,
    extras_require=EXTRAS,
    scripts=[])
