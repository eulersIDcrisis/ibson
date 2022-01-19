"""setup.py for ibson.

Installing ibson.
"""
from setuptools import setup


VERSION = '0.1.0'


setup(
    version=VERSION,
    keywords='bson',
    install_requires=[
    ],
    setup_requires=['flake8'],
    entry_points={
        # 'console_scripts': [
        #     'bsonq='
        # ]
    }
)
