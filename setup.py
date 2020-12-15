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
    install_requires=[
        'flattentool',
        'google-colab',
        'gspread<3.5.0',  # google-colab 1.0.0 requires google-auth~=1.4.0
        'gspread-dataframe',
        'libcoveocds',
        'notebook~=5.2.0',  # google-colab 1.0.0
        'oauth2client',
        'pydrive',
        'requests~=2.21.0',  # google-colab 1.0.0
        'ipython-sql~=0.4.0',
        # https://github.com/googleapis/google-api-python-client/issues/870
        'google-api-python-client!=1.8.1,<1.9.0',  # google-colab 1.0.0 requires google-auth~=1.4.0

        # Avoid conflicts.
        'Django<2.3',  # libcove 0.18.0
        'google-api-core<1.17.0',  # google-colab 1.0.0 requires google-auth~=1.4.0
    ],
    extras_require={
        'test': [
            'coveralls',
            'pytest',
            'pytest-cov',
            'pandas~=0.24.0',  # google-colab 1.0.0
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
