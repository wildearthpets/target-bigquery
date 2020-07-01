#!/usr/bin/env python

from setuptools import setup

setup(name='target-bigquery',
      version='1.4.0',
      description='Singer.io target for writing data to Google BigQuery',
      author='RealSelf Business Intelligence',
      url='https://github.com/RealSelf/target-bigquery',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['target_bigquery'],
      install_requires=[
          'jsonschema==2.6.0',
          'simplejson==3.11.1',
          'singer-python>=1.5.0',
          'google-api-python-client>=1.6.2',
          'google-cloud>=0.34.0',
          'google-cloud-bigquery>=1.9.0',
          'oauth2client', 'boto3',
          'google-cloud-storage>=1.25.0'
      ],
      entry_points='''
          [console_scripts]
          target-bigquery=target_bigquery:main
      ''',
)
