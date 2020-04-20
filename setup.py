from setuptools import setup

setup(
    name='ocdskingfishercolab',
    version='0.0.1',
    author='Open Contracting Partnership',
    author_email='data@open-contracting.org',
    license='BSD',
    packages=['kingfishercolab'],
    package_data={},
    install_requires=[
        'SQLAlchemy',
        'alembic',
        'psycopg2-binary',
        'gspread',
        'requests',
        'flattentool',
        'gspread-dataframe',
        'pydrive',
    ],
    entry_points='''
    ''',
)
