#!/usr/bin/env python
# coding: utf-8

from setuptools import setup

with open("README.md", "r",encoding="utf8") as fh:
    long_description = fh.read()

setup(
    name='one_quant_data',
    version='0.1.6',
    author='onewayforever',
    author_email='onewayforever@163.com',
    url='https://github.com/onewayforever/one-quant-data',
    description=u'Data engine for stockA quant',
    long_description=long_description,
    long_description_content_type="text/markdown",
    #packages=find_packages(),
    packages=['one_quant_data'],
    install_requires=[
        'tushare>=1.2.26',
        'progressbar',
        'pandas',
        'pymysql',
        'sqlalchemy',
        'numpy'
    ]
)
