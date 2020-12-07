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
        'scalems': ['py.typed']
    },
    url='https://github.com/SCALE-MS/scale-ms/',
    license='',
    author='SCALE-MS team',
    author_email='',
    description='SCALE-MS data flow scripting and graph execution for molecular science computational research protocols.'
)
