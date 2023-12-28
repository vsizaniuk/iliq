from setuptools import setup

setup(
    name='iliq',
    version='0.0.1',
    description='Interactive interface for Liquibase project',
    author='Vladimir Sizaniuk',
    author_email='vsizaniuk@gmail.com',

    install_requires=[
        'psycopg~=3.1.13; python_version == "3.10"',
    ],

    entry_points={
        'console_scripts': [
            'iliq = iliq.liqui:cli_startup',
        ]
    }
)