import os
import sys
import subprocess

import distutils.cmd
from setuptools import setup, find_packages
from setuptools.command.install import install
from setuptools.command.develop import develop

v = sys.version_info
if sys.version_info < (3, 8):
    msg = "FAIL: Requires Python 3.8 or later, " \
          "but setup.py was run using {}.{}.{}"
    v = sys.version_info
    print(msg.format(v.major, v.minor, v.micro))
    # noinspection PyPackageRequirements
    print("NOTE: Installation failed. Run setup.py using python3")
    sys.exit(1)

try:
    here = os.path.abspath(os.path.dirname(__file__))
except NameError:
    # it can be the case when we are being run as script or frozen
    here = os.path.abspath(os.path.dirname(sys.argv[0]))

metadata = {'__file__': os.path.join(here, 'plenum', '__metadata__.py')}
with open(metadata['__file__'], 'r') as f:
    exec(f.read(), metadata)

tests_require = ['attrs==20.3.0', 'pytest==6.2.5', 'pytest-xdist==2.2.1', 'pytest-forked==1.3.0',
                 'python3-indy==1.16.0.post236', 'pytest-asyncio==0.14.0']


class PyZMQCommand(distutils.cmd.Command):
    description = 'pyzmq install target'

    version = 'pyzmq==22.3.0'
    options = '--install-option=--zmq=bundled'

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        command = ['pip', 'install', self.version, self.options]
        subprocess.check_call(command)


class InstallCommand(install):
    description = 'install target'

    def run(self):
        install.run_command(self, command='pyzmq')
        install.run(self)


class DevelopCommand(develop):
    description = 'develop target'

    def run(self):
        develop.run_command(self, command='pyzmq')
        develop.run(self)


setup(
    cmdclass={
        'install': InstallCommand,
        'develop': DevelopCommand,
        'pyzmq': PyZMQCommand,
    },
    name=metadata['__title__'],
    version=metadata['__version__'],
    author=metadata['__author__'],
    author_email=metadata['__author_email__'],
    maintainer=metadata['__maintainer__'],
    maintainer_email=metadata['__maintainer_email__'],
    url=metadata['__url__'],
    description=metadata['__description__'],
    long_description=metadata['__long_description__'],
    download_url=metadata['__download_url__'],
    license=metadata['__license__'],
    classifiers=[
        "Programming Language :: Python :: 3"
    ],

    keywords='Byzantine Fault Tolerant Plenum',
    packages=find_packages(exclude=['test', 'test.*', 'docs', 'docs*', 'simulation']) + [
        'data', ],
    # TODO move that to MANIFEST.in
    package_data={
        '': ['*.txt', '*.md', '*.rst', '*.json', '*.conf', '*.html',
             '*.css', '*.ico', '*.png', 'LICENSE', 'LEGAL', 'plenum']},
    include_package_data=True,

    install_requires=[
                        # 'base58==2.1.0',
                        'base58',
                        # pinned because issue with fpm from v4.0.0
                        'importlib_metadata==3.10.1',
                        # 'ioflo==2.0.2',
                        'ioflo',
                        # 'jsonpickle==2.0.0',
                        'jsonpickle',
                        # 'leveldb==0.201',
                        'leveldb',
                        # Pinned because of changing size of `crypto_sign_SECRETKEYBYTES` from 32 to 64
                        'libnacl==1.6.1',
                        # 'msgpack-python==0.5.6',
                        'msgpack-python',
                        # 'orderedset==2.0.3',
                        'orderedset',
                        # 'packaging==20.9',
                        'packaging',
                        # Pinned because 3rd party dependency build fails with portalocker > 2.7.0',
                        'portalocker==2.7.0',
                        'prompt_toolkit>=3.0.18',
                        # 'psutil==5.6.6',
                        'psutil',
                        # Pinned because tests fail with v.0.9
                        'pympler==0.8',
                        # 'python-dateutil==2.8.1',
                        'python-dateutil',
                        # 'python-rocksdb==0.7.0',
                        'python-rocksdb',
                        'python-ursa==0.1.1',
                        ### Tests fail without version pin (GHA run: https://github.com/udosson/indy-plenum/actions/runs/1078745445)
                        'rlp==2.0.0',
                        'semver==2.13.0',
                        # 'sha3==0.2.1',
                        'sha3',
                        # 'six==1.15.0',
                        'six',
                        ### Tests fail without version pin (GHA run: https://github.com/udosson/indy-plenum/actions/runs/1078741118)
                        'sortedcontainers==2.1.0',
                        ### Tests fail without version pin (GHA run: https://github.com/udosson/indy-plenum/actions/runs/1078741118)
                        'ujson==1.33',
                        ],

    setup_requires=['pytest-runner==5.3.0'],
    extras_require={
        'tests': tests_require,
        'stats': ['python-firebase'],
        'benchmark': ['pympler==0.8']
    },
    tests_require=tests_require,
    scripts=['scripts/init_plenum_keys',
             'scripts/start_plenum_node',
             'scripts/generate_plenum_pool_transactions',
             'scripts/gen_steward_key', 'scripts/gen_node',
             'scripts/export-gen-txns', 'scripts/get_keys',
             'scripts/udp_sender', 'scripts/udp_receiver', 'scripts/filter_log',
             'scripts/log_stats',
             'scripts/init_bls_keys',
             'scripts/process_logs/process_logs',
             'scripts/process_logs/process_logs.yml']
)
