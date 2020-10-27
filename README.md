# SCALE-MS

[![Build Status](https://travis-ci.com/SCALE-MS/scale-ms.svg?branch=master)](https://travis-ci.com/SCALE-MS/scale-ms)
[![Documentation Status](https://readthedocs.org/projects/scale-ms/badge/?version=latest)](https://scale-ms.readthedocs.io/en/latest/?badge=latest)
[[DockerHub]](https://hub.docker.com/r/scalems/ci)

NSF Awards

* [1835449](https://www.nsf.gov/awardsearch/showAward?AWD_ID=1835449) Matteo Turilli, Rutgers University New Brunswick
* [1835607](https://www.nsf.gov/awardsearch/showAward?AWD_ID=1835607) Kristen Fichthorn, Pennsylvania State University
* [1835720](https://www.nsf.gov/awardsearch/showAward?AWD_ID=1835720) Michael Shirts, University of Colorado at Boulder
* [1835780](https://www.nsf.gov/awardsearch/showAward?AWD_ID=1835780) Peter Kasson, University of Virginia

## Downloading

### Git

This project is managed with a `git` repository hosted at https://github.com/SCALE-MS/scale-ms

### Test data submodule

Data for some examples and tests is stored in a separate
[testdata repository](https://github.com/SCALE-MS/testdata),
but the external repository is embedded as a git submodule for convenience.
(`testdata/`)
This fact should be uninteresting to most readers,
but if you need the extra files, you will appreciate the convenience of a couple of
extra arguments available to `git` commands.
* When cloning this repository, use the `--recursive` argument to `git clone`.
* When updating a local copy of this repository, use the `--recurse-submodules` argument to `git pull` or `git checkout`.