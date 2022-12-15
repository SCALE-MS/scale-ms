# SCALE-MS

[![Test Status](https://github.com/SCALE-MS/scale-ms/actions/workflows/tests.yml/badge.svg?branch=master)](https://github.com/SCALE-MS/scale-ms/actions/workflows/tests.yml)
[![Documentation Status](https://readthedocs.org/projects/scale-ms/badge/?version=latest)](https://scale-ms.readthedocs.io/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/SCALE-MS/scale-ms/branch/master/graph/badge.svg)](https://codecov.io/gh/SCALE-MS/scale-ms)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
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

## Documentation

Refer to https://scale-ms.readthedocs.io/en/latest/ for the most up-to-date documentation.

To build the documentation locally, clone this repository and go to the root directory of the local repository, then
1. Install [Sphinx](https://www.sphinx-doc.org/en/master/) and additional requirements:
   `pip install -r docs/requirements.txt`
2. Install the `scalems` package so that Sphinx can import it when run: `pip install .`
3. Optionally, install [plantuml](https://plantuml.com/starting) for rendering diagrams.
   (e.g. on Ubuntu, do `sudo apt-get install plantuml`)

Build with Sphinx using the `docs/conf.py` file in this repository.

### Example
From the root of a local copy of the repository, build HTML documentation for the
`scalems` package into a new `./web` directory (or some directory of your choosing).
`./docs` is the input directory.

    python3 -m sphinx docs web -b html

Then open `./web/index.html`

Note: Depending on your Sphinx installation, you may have a `sphinx-build` executable
or you may access the `sphinx` module with the `-m` Python interpreter option
(`python -m sphinx ...`).
