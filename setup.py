import build
import os
import pathlib
import sys

from setuptools import setup

# If the Python version is too low, developers will get a strange error when versioneer
# tries to import scalems._version since scalems/__init__.py imports some submodules
# with dependencies on recently-introduced standard library modules.
_supported = True
if sys.version_info.major < 3:
    _supported = False
if sys.version_info.major == 3 and sys.version_info.minor < 8:
    _supported = False
if not _supported:
    raise RuntimeError('scalems requires Python 3.8 or higher.')

# The versioneer.py is in the same directory as the setup.py
sys.path.append(os.path.dirname(__file__))
import versioneer

from setuptools.command.build_py import build_py as _build_py


_src_dir = pathlib.Path(__file__).parent


class cmd_build_py(_build_py):
    def build_package_data(self):
        _build_py.build_package_data(self)
        build_dir = os.path.join(self.build_lib, 'scalems', 'data')
        dist = build.ProjectBuilder(str(_src_dir)).build(distribution='sdist', output_directory=build_dir)
        print(f'Generated {dist}')


# PEP-517 package details are in setup.cfg, but we keep this file for non-PEP-517 capable
# installations, including "editable" installations with `pip install -e` or `setup.py develop`
setup(cmdclass=versioneer.get_cmdclass({'build_py': cmd_build_py}))
# setup(cmdclass=versioneer.get_cmdclass())
