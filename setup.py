#!/usr/bin/env python
#coding=utf8

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

setup(
        name = 'novel_crawler',
        version = '1.0',
        install_requires = [
                'sqlalchemy',
                'mysqlclient',
                'requests', 
                'redis'],
        description = 'pintuanya job',
        url = 'https://code.aliyun.com/zhouxg2/novel_crawler.git', 
        author = 'zhouxianggen',
        author_email = 'zhouxianggen@gmail.com',
        classifiers = [ 'Programming Language :: Python :: 3.7',],
        packages = find_packages(),
        data_files = [
                ('/conf/supervisor/program/', ['conf/novel_crawler.ini']),
                ('/conf/novel_crawler/', ['conf/novel_crawler.conf']),
                ],
        entry_points = {
                'console_scripts': [
                        'run_novel_crawlers = novel_crawler.run:main',
                        ]
                }
        )

