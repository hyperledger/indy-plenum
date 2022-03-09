# GitHub Actions Workflow

The workflow in the [push_pr.yaml](push_pr.yaml) file runs on push and pull requests to the ubuntu-20-04-upgrade branch.
It uses the following reusable workflows in thisfolder.

+ [reuseable_lint.yaml](reuseable_lint.yaml)
   This workflow runs the linting with flake8.
+ [reuseable_buildimage.yaml](reuseable_buildimage.yaml)
   This workflow builds the dockerimages and pushes them to the GHCR.
+ [reuseable_test.yaml](reuseable_test.yaml)
   This workflow runs the tests inside the uploaded docker images.
+ [reusable_buildpackages.yaml](reusable_buildpackages.yaml)
   This workflows builds the python and debian packages. It also uploads them to the workflow.
+ [reuseable_publish_artifacts.yaml](reuseable_publish_artifacts.yaml)
   This workflow uploads the packages to PYPI and Artifactory.