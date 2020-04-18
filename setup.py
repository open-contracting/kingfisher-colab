from setuptools import find_packages, setup

setup(
    name='ocdskingfishercolab',
    version='0.0.0',
    packages=find_packages(),
    install_requires=[
        'alembic',
        'flattentool',
        'gspread',
        'gspread-dataframe',
        'psycopg2-binary',
        'pydrive',
        'requests',
        'SQLAlchemy',
    ],
    extras_require={
        'docs': [
            'Sphinx',
            'sphinx-autobuild',
            'sphinx_rtd_theme',
        ],
    },
)
