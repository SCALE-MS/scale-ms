from setuptools import find_packages, setup

setup(
    name='scalems',
    version='0',
    # Python 3.8 incorporates some valuable typing helpers such as
    # typing.Protocol (PEP 544), typing.TypedDict (PEP 589), and
    # https://docs.python.org/3.8/library/functools.html#functools.singledispatchmethod.
    # We require 3.8 for early development to reduce boiler-plate,
    # back-ports, and workarounds for faster delivery.
    # If necessary, we can back-port to Python 3.7.
    python_requires='>=3.8',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    package_data={
        # Enable PEP-561 style type hinting and .pyi type hinting files.
        # Bundle some non-package scripts and config files.
        'scalems': ['py.typed',
                    'radical/data/*.json'],
    },
    entry_points={
        'console_scripts': [
            'scalems_rp_agent=scalems.radical.scalems_rp_agent:main',
            'scalems_rp_worker=scalems.radical.scalems_rp_worker:main',
        ],
    },
    # Somehow, this breaks the RP prepare_env:
    install_requires=['radical.pilot @ git+https://github.com/radical-cybertools/radical.pilot.git@project/scalems'],
    url='https://github.com/SCALE-MS/scale-ms/',
    license='',
    author='SCALE-MS team',
    author_email='',
    description='SCALE-MS data flow scripting and graph execution for molecular science computational research protocols.'
)
