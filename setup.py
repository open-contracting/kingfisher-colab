from setuptools import find_packages, setup

setup(
    name='ocdskingfishercolab',
    version='0.0.0',
    packages=find_packages(),
    install_requires=[
        'alembic',
        'flattentool',
        'google-colab',
        'gspread',
        'gspread-dataframe',
        'psycopg2-binary',
        'pydrive',
        'requests',
        'SQLAlchemy',
    ],
    extras_require={
        'test': [
            'pytest',
        ],
        'docs': [
            'Sphinx',
            'sphinx-autobuild',
            'sphinx_rtd_theme',
        ],
    },
)
