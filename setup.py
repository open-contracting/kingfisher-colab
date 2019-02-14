from setuptools import setup

setup(
    name='kingfisher-colab',
    version='0.0.2',
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
