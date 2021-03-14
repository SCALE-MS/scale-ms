import os
import sys
from setuptools import setup

# The versioneer.py is in the same directory as the setup.py
sys.path.append(os.path.dirname(__file__))
import versioneer

# PEP-517 package details are in setup.cfg, but we keep this file for non-PEP-517 capable
# installations, including "editable" installations with `pip install -e` or `setup.py develop`
setup(cmdclass=versioneer.get_cmdclass())
