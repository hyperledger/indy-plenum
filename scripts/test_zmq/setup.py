#!/usr/bin/env python
import distutils
import os
import subprocess
import sys
from setuptools import setup, find_packages
from setuptools.command.install import install
from setuptools.command.develop import develop

v = sys.version_info
if sys.version_info < (3, 5):
    msg = "FAIL: Requires Python 3.5 or later, " \
          "but setup.py was run using {}.{}.{}"
    v = sys.version_info
    print(msg.format(v.major, v.minor, v.micro))
    # noinspection PyPackageRequirements
    print("NOTE: Installation failed. Run setup.py using python3")
    sys.exit(1)

# resolve metadata
metadata = {}
here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'test_zmq', '__version__.py'), 'r') as f:
    exec(f.read(), metadata)


class PyZMQCommand(distutils.cmd.Command):
    description = 'pyzmq install target'

    version = 'pyzmq==18.1.0'
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
    description=metadata['__description__'],
    long_description=metadata['__long_description__'],
    keywords=metadata['__keywords__'],
    url=metadata['__url__'],
    author=metadata['__author__'],
    author_email=metadata['__author_email__'],
    maintainer=metadata['__maintainer__'],
    license=metadata['__license__'],
    packages=find_packages(),
    package_data={'': ['*.md']},
    include_package_data=True,
    install_requires=['libnacl==1.6.1', 'base58==1.0.0'],
    scripts=['test_zmq/server.py', 'test_zmq/client.py']
)
