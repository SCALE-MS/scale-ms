from setuptools import find_packages, setup

setup(
    name='scalems',
    version='0',
    python_requires='>=3.7',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    url='https://github.com/SCALE-MS/scale-ms/',
    license='',
    author='SCALE-MS team',
    author_email='',
    description='SCALE-MS data flow scripting and graph execution for molecular science computational research protocols.'
)
