#!/usr/bin/env python
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

# resolve metadata
metadata = {}
here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'test_zmq', '__version__.py'), 'r') as f:
    exec(f.read(), metadata)


setup(
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
    install_requires=['libnacl==1.6.1', 'base58==1.0.0', 'pyzmq==18.1.1'],
    scripts=['test_zmq/server.py', 'test_zmq/client.py']
)
