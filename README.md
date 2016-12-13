# packtivity

[![Build Status](https://travis-ci.org/diana-hep/packtivity.svg?branch=master)](https://travis-ci.org/diana-hep/packtivity)
[![PyPI](https://img.shields.io/pypi/v/packtivity.svg)](https://pypi.python.org/pypi/packtivity)

This package aims to collect implement the both synchronous and asynchronous execution of preserved scientific computation that come with batteries included, i.e. with a full specification of their software dependencies. In that sense they are *packaged activities* -- packtivities.

This package provides tools to validate and execute data processing tasks that are written according to the "packtivity" JSON schemas defined in https://github.com/diana-hep/cap-schemas.

Packtivities define

* the software environment
* parametrized process descriptions (what programs to run within these environment) and
* produces human and machine readable outputs (as JSON) of the resulting data fragments.

## Example Packtivity spec

    process:
      process_type: 'string-interpolated-cmd'
      cmd: 'DelphesHepMC  {delphes_card} {outputroot} {inputhepmc} && root2lhco {outputroot} {outputlhco}'
    publisher:
      publisher_type: 'frompar-pub'
      outputmap:
        lhcofile: outputlhco
        rootfile: outputroot
    environment:
      environment_type: 'docker-encapsulated'
      image: lukasheinrich/root-delphes

## Packtivity in Yadage

This package is used by https://github.com/lukasheinrich/yadage to execute the individual steps of yadage workflows.
