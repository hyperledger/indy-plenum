#!/bin/bash

set -e

usage_str="Usage: $0"

if [ "$1" = "--help" ] ; then
  echo $usage_str
  exit 0
fi

repourl=$(git config --get remote.origin.url)
hashcommit=$(git $repo rev-parse HEAD)

python3 -c "import plenum; plenum.set_manifest({'repo': '$repourl', 'sha1': '$hashcommit', 'version': plenum.__version__})"
