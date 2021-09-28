import os
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

# PEP-517 package details are in setup.cfg, but we keep this file for non-PEP-517 capable
# installations, including "editable" installations with `pip install -e` or `setup.py develop`
setup(cmdclass=versioneer.get_cmdclass())
