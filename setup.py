import build
import os
import pathlib
import sys

from setuptools import setup

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
