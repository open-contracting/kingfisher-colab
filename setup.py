from setuptools import find_packages, setup

with open('README.rst') as f:
    long_description = f.read()

setup(
    name='ocdskingfishercolab',
    version='0.3.5',
    author='Open Contracting Partnership',
    author_email='data@open-contracting.org',
    url='https://github.com/open-contracting/kingfisher-colab',
    description='A set of utility functions for Google Colaboratory notebooks using OCDS data',
    license='BSD',
    packages=find_packages(exclude=['tests', 'tests.*']),
    long_description=long_description,
    long_description_content_type='text/x-rst',
    # google-colab on PyPI has different requirements than google-colab on Google Colaboratory. It's not possible to
    # list a set of requirements that pip can resolve in both environments.
    install_requires=[
        'flattentool',
        'gspread',
        'gspread-dataframe',
        'ipython',
        'ipython-sql~=0.4.0',  # Google Colaboratory uses 0.3.x
        'libcoveocds',
        'notebook',
        'oauth2client',
        'pydrive',
        'requests',
        'sqlalchemy',
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
            'sphinx-rtd-theme',
        ],
    },
    classifiers=[
        'License :: OSI Approved :: BSD License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3.6',
    ],
)
