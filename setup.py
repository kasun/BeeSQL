#!/usr/bin/env python

from distutils.core import setup

setup(name='BeeSQL',
    version='0.1',
    description='Pythonic SQL library',
    author='Kasun Herath',
    author_email='kasunh01@gmail.com',
    packages=[
        'beesql', 
        'beesql.backends',
        ],
    )


