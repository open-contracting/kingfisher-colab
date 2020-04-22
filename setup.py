from setuptools import find_packages, setup

with open('README.rst') as f:
    long_description = f.read()

setup(
    name='ocdskingfishercolab',
    version='0.2.2',
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
        'gspread',
        'gspread-dataframe',
        'libcoveocds',
        'notebook',
        'oauth2client',
        'pandas',
        'psycopg2-binary',
        'pydrive',
        'requests',
        # https://github.com/googleapis/google-api-python-client/issues/870
        'google-api-python-client!=1.8.1',
    ],
    extras_require={
        'test': [
            'coveralls',
            'pytest',
            'pytest-cov',
        ],
        'docs': [
            'Sphinx',
            'sphinx-autobuild',
            'sphinx_rtd_theme',
        ],
    },
    classifiers=[
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3.6',
    ],
)
