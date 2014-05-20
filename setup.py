#!python
# -*- coding: utf-8 -*-
from setuptools import setup

readme = open('readme.md', 'rU').read()

setup(
    name='geowiki',
    version='0.1.0',
    description='Geo Coding module',
    long_description=readme,
    url='https://gerrit.wikimedia.org/r/gitweb?p=analytics/editor-geocoding.git;a=summary',

    author='Declerambaul',
    author_email='{otto,dvanliere,fkaelin,dsc,erosen}@wikimedia.org',

    packages=['geowiki'],
    # package_dir={'geowiki': 'geowiki'},

    package_data={'geowiki': ['data/*.bots', 'data/*.csv']},
    include_package_data=True,
    install_requires=[
        "argparse >=1.2.1",
        "MySQL-python >= 1.2.3",
        "geoip",
        "gcat == 0.1.0",
    ],
    entry_points={
        'console_scripts': ['geowiki=geowiki.process_data:main']
    },
    dependency_links=[
        "http://github.com/wikimedia/limnpy/tarball/master#egg=limnpy-0.1.0",
        "http://github.com/embr/wikipandas/tarball/master#egg=wikipandas-0.0.1"
    ],
    classifiers=[],
)
