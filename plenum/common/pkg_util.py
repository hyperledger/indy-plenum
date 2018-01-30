import re
import sys
from importlib import import_module

import semver

MAJOR_MINOR_REGEX = re.compile(r"\d+\.\d+")


def getPackageMeta(pkg):
    try:
        meta = import_module('{}.__metadata__'.format(pkg))
    except ImportError:
        print("A dependency named {} is not installed. Installation cannot "
              "proceed without it.".format(pkg))
        sys.exit(1)
    return meta


def check_deps(dependencies, parent=""):
    if isinstance(dependencies, dict):
        for pkg_name, exp_ver in dependencies.items():
            if parent:
                full_name = "{} ({})".format(pkg_name, parent)
            else:
                full_name = pkg_name
            meta = getPackageMeta(pkg_name)
            ver = meta.__version__
            if MAJOR_MINOR_REGEX.fullmatch(ver):
                ver += ".0"  # Add a fictive patch number to fit semver format
            if not semver.match(ver, exp_ver):
                raise RuntimeError("Incompatible '{}' package version. "
                                   "Expected: {} "
                                   "Found: {}".
                                   format(pkg_name, exp_ver, ver))
            if hasattr(meta, "__dependencies__"):
                deps = meta.__dependencies__
                check_deps(deps, full_name)
    else:
        pkg = dependencies if isinstance(dependencies, str) else \
            dependencies.__name__
        meta = getPackageMeta(pkg)
        deps = meta.__dependencies__
        check_deps(deps)


def update_module_vars(module_path, **kwargs):
    module = import_module(module_path)
    module.__dict__.update(kwargs)
