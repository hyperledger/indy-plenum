#!/bin/bash -ex

if [ "$1" = "--help" ] ; then
  echo "Usage: $0 <path-to-repo-folder> <main-module-name> <release-version-dotted> <distro-packages>"
  echo "<distro-packages> - Set to 'debian-packages' when preparing deb packages, and 'python-packages' when preparing PyPi packages."
  exit 0
fi

repo="$1"
module_name="$2"
version_dotted="$3"
distro_packages="$4"

BUMP_SH_SCRIPT="bump_version.sh"
GENERATE_MANIFEST_SCRIPT="generate_manifest.sh"

pushd $repo

echo -e "\nSetting version to $version_dotted"
bash -ex $BUMP_SH_SCRIPT $version_dotted
cat $module_name/__version__.json

echo -e "\nGenerating manifest"
bash -ex $GENERATE_MANIFEST_SCRIPT
cat $module_name/__manifest__.json

if [ "$distro_packages" = "debian-packages" ]; then
  # Only used for the deb package builds, NOT for the PyPi package builds.
  # Update the package names to match the versions that are pre-installed on the os.
  echo -e "\nAdapt the dependencies for the Canonical archive"
  #### ToDo adjust packages for the Cannonical archive for Ubuntu 20.04 (focal)
  # sed -i "s~ujson==1.33~ujson==1.33-1build1~" setup.py
  # sed -i "s~prompt_toolkit==0.57~prompt_toolkit==0.57-1~" setup.py
  # sed -i "s~msgpack-python==0.4.6~msgpack==0.4.6-1build1~" setup.py
  sed -i "s~msgpack-python~msgpack~" setup.py
elif [ "$distro_packages" = "python-packages" ]; then
  echo -e "\nNo adaption of dependencies for python packages"
else
  echo -e "\nNo distribution specified. Please, specify distribution as 'debian-packages' or 'python-packages'."
  exit 1
fi

popd

echo -e "\nFinished preparing $repo for publishing\n"