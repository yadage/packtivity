
from setuptools import setup, find_packages

setup(
  name = 'packtivity',
  version = '0.5.2',
  description = 'packtivity - general purpose schema + bindings for PROV activities',
  url = '',
  author = 'Lukas Heinrich',
  author_email = 'lukas.heinrich@cern.ch',
  packages = find_packages(),
  include_package_data = True,
  install_requires = [
      'requests[security]',
      'jsonschema',
      'jsonref',
      'pyyaml',
      'functools32',
      'click',
      'psutil',
      'cap-schemas'
  ],
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
