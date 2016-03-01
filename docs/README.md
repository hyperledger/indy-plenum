# Installing Sphinx:

Navigate to your virtual environment directory and execute the following commands.

```
source bin/activate

pip3 install sphinx
```

# Building documentation:

To build api-docs, run `sphinx-apidoc -e -E -f -l -o docs/source/api_docs plen um` from the project root.

To build html documentation, run `make html` from the docs directory. The generated docs can be found in _build_ directory.

Alternative command to build html documentation (to be run from the project root): `sphinx-build -b html docs docs/build`

Open index.html under _docs/build/html_ to view the documentation.