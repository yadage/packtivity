
from setuptools import setup, find_packages

setup(
  name = 'packtivity',
  version = '0.0.4',
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
  entry_points = {
      'console_scripts': [
          'packtivity-run=packtivity.cli:runcli',
          'packtivity-validate=packtivity.cli:validatecli'
      ],
  },
  dependency_links = [
  ]
)
