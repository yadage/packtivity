# packtivity

[![DOI](https://zenodo.org/badge/53696818.svg)](https://zenodo.org/badge/latestdoi/53696818)
[![Coverage Status](https://coveralls.io/repos/github/diana-hep/packtivity/badge.svg)](https://coveralls.io/github/diana-hep/packtivity)
[![Documentation Status](https://readthedocs.org/projects/packtivity/badge/?version=latest)](http://packtivity.readthedocs.io/en/latest/?badge=latest)
[![PyPI](https://img.shields.io/pypi/v/packtivity.svg)](https://pypi.python.org/pypi/packtivity)

This package aims to collect implementations of both synchronous and asynchronous execution of preserved, but parametrized scientific computational tasks that come with batteries included, i.e. with a full specification of their software dependencies. In that sense they are *packaged activities* -- packtivities.

This package provides tools to validate and execute data processing tasks that are written according to the "packtivity" JSON schemas defined in [`yadage-schemas`](https://github.com/yadage/yadage-schemas).

Packtivities define

* the software environment
* parametrized process descriptions (what programs to run within these environment) and
* produces human and machine readable outputs (as JSON) of the resulting data fragments.

At run-time they are paired with a concrete set of parameters supplied as JSON documents and and external storage/state to actually execute these tasks.

## Packtivity in Yadage

This package is used by [`yadage`](https://github.com/yadage/yadage) to execute the individual steps of yadage workflows.

## Example Packtivity spec

This packtivity spec is part of a number of yadage workflow and runs the Delphes detector simulation on a HepMC file and outputs events in the LHCO and ROOT file formats. This packtivity is [stored in a public location](https://github.com/lukasheinrich/yadage-workflows/blob/8422d9fe8e21f709243cbb47b5adb66f2e432e51/phenochain/delphes.yml) from which it can be later retrieved:

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

## Usage

You can run the packtivity in a synchronous way by specifying the spec (can point to GitHub),  all necessary parameters and attaching an external state (via the `--read` and `--write` flags).

    packtivity-run -t from-github/phenochain delphes.yml \
      -p inputhepmc="$PWD/pythia/output.hepmc" \
      -p outputroot="'{workdir}/output.root'" \
      -p outputlhco="'{workdir}/output.lhco'" \
      -p delphes_card=delphes/cards/delphes_card_ATLAS.tcl \
      --read pythia --write outdir

## Asynchronous Backends

In order to facilitate usage of distributed resources, a number of Asynchronous
backends can be specified. Here is an example for IPython Parallel clusters

    packtivity-run -b ipcluster --asyncwait \
      -t from-github/phenochain delphes.yml \
      -p inputhepmc="$PWD/pythia/output.hepmc" \
      -p outputroot="'{workdir}/output.root'" \
      -p outputlhco="'{workdir}/output.lhco'" \
      -p delphes_card=delphes/cards/delphes_card_ATLAS.tcl \
      --read pythia --write outdir

You can replacing the `--asyncwait` with `--async` flag in order to get a JSONable proxy representation with which to later on check on the job status. By default the proxy information is written to `proxy.json` (customizable via the `-x` flag):

    packtivity-run -b celery --async \
      -t from-github/phenochain delphes.yml \
      -p inputhepmc="$PWD/pythia/output.hepmc" \
      -p outputroot="'{workdir}/output.root'" \
      -p outputlhco="'{workdir}/output.lhco'" \
      -p delphes_card=delphes/cards/delphes_card_ATLAS.tcl \
      --read pythia --write outdir

And at a later point in time you can check via:

    packtivity-checkproxy proxy.json

## External Backends

Users can implement their own backends to handle the JSON documents describing the packtivities. It can be enabled
by using the `fromenv` backend and setting an environment variable specifying the module holding the backend and proxy
classes. The format of the environment variable is `module:backendclass:proxyclass`. E.g.:

    export PACKTIVITY_ASYNCBACKEND="externalbackend:ExternalBackend:ExternalProxy"
