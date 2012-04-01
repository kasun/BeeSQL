#!/usr/bin/env python

from distutils.core import setup

setup(name='BeeSQL',
    version='1.0',
    description='Pythonic SQL library',
    author='Kasun Herath',
    author_email='kasunh01@gmail.com',
    packages=[
        'beesql', 
        'beesql.backends',
        ],
    )


