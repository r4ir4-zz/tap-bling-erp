#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-vnda-ecommerce",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_vnda_ecommerce"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        "singer-python",
        "requests",
    ],
    entry_points="""
    [console_scripts]
    tap-vnda-ecommerce=tap_vnda_ecommerce:main
    """,
    packages=["tap_vnda_ecommerce"],
    package_data = {
        "schemas": ["tap_vnda_ecommerce/schemas/*.json"]
    },
    include_package_data=True,
)
