from setuptools import find_packages, setup

with open('README.rst') as f:
    long_description = f.read()

setup(
    name='ocdskingfishercolab',
    version='0.3.0',
    author='Open Contracting Partnership',
    author_email='data@open-contracting.org',
    url='https://github.com/open-contracting/kingfisher-colab',
    description='A set of utility functions for Google Colaboratory notebooks using OCDS data',
    license='BSD',
    packages=find_packages(exclude=['tests', 'tests.*']),
    long_description=long_description,
    # google-colab on PyPi has different requirements than google-colab on Google Colaboratory. It's not possible to
    # list a set of requirements that pip can resolve in both environments.
    install_requires=[
        'flattentool',
        'gspread',
        'gspread-dataframe',
        'ipython',
        'ipython-sql',
        'libcoveocds',
        'notebook',
        'oauth2client',
        'pydrive',
        'requests',
    ],
    extras_require={
        'test': [
            'coveralls',
            'pytest',
            'pytest-cov',
            'pandas',
            'psycopg2-binary',
        ],
        'docs': [
            'Sphinx',
            'sphinx-autobuild',
            'sphinx_rtd_theme',
        ],
    },
    classifiers=[
        'License :: OSI Approved :: BSD License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3.6',
    ],
)
