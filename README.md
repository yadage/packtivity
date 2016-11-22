# packtivity

[![Build Status](https://travis-ci.org/diana-hep/packtivity.svg?branch=master)](https://travis-ci.org/diana-hep/packtivity)
[![PyPI](https://img.shields.io/pypi/v/packtivity.svg)](https://pypi.python.org/pypi/packtivity)

This package mainly provides tools to validate and execute data processing tasks that are defined via the "packtivity" JSON schemas defined in https://github.com/diana-hep/cap-schemas. Packtivities define the software environment, parametrized process descriptions (what programs to run within these environement) and produces human and machine readable outputs (as JSON) of the resulting data fragments.

    packtivity-run dummy_analysis/dummystep.yml -s from-github
    packtivity-validate dummy_analysis/dummystep.yml -s from-github


## Packtivity in Yadage

this package is used by https://github.com/lukasheinrich/yadage to execute the individual steps of yadage workflows.

