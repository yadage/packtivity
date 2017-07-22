
import os
from setuptools import setup, find_packages

deps = [
      'requests[security]',
      'jsonschema',
      'jsonref',
      'pyyaml',
      'functools32',
      'click',
      'glob2',
      'jsonpointer',
      'psutil',
      'yadage-schemas',
      'mock',
      'checksumdir',
]

if not 'READTHEDOCS' in os.environ:
  deps += ['jq']



setup(
  name = 'packtivity',
  version = '0.6.2',
  description = 'packtivity - general purpose schema + bindings for PROV activities',
  url = '',
  author = 'Lukas Heinrich',
  author_email = 'lukas.heinrich@cern.ch',
  packages = find_packages(),
  include_package_data = True,
  install_requires = deps,
  extras_require={
   'celery':  [
       'celery'
    ],
  },
  entry_points = {
      'console_scripts': [
          'packtivity-run=packtivity.cli:runcli',
          'packtivity-validate=packtivity.cli:validatecli',
          'packtivity-checkproxy=packtivity.cli:checkproxy'
      ],
  },
  dependency_links = [
  ]
)
