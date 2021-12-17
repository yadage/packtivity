import os
from setuptools import setup, find_packages

deps = [
    "requests[security]",
    "jsonschema",
    "jsonref",
    "pyyaml",
    "click",
    "glob2",
    "jsonpointer",
    "jsonpath-rw",
    "jq",
    "yadage-schemas",
    "mock",
    "checksumdir",
]

if not "READTHEDOCS" in os.environ:
    deps += ["jq"]


setup(
    name="packtivity",
    version="0.14.24",
    description="packtivity - general purpose schema + bindings for PROV activities",
    url="https://github.com/yadage/packtivity",
    author="Lukas Heinrich",
    author_email="lukas.heinrich@cern.ch",
    packages=find_packages(),
    python_requires=">=3.6",
    include_package_data=True,
    install_requires=deps,
    extras_require={"celery": ["celery", "redis"]},
    entry_points={
        "console_scripts": [
            "packtivity-run=packtivity.cli:runcli",
            "packtivity-util=packtivity.cli:utilcli",
            "packtivity-validate=packtivity.cli:validatecli",
            "packtivity-checkproxy=packtivity.cli:checkproxy",
        ],
    },
    dependency_links=[],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: Implementation :: CPython",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Physics",
        "Operating System :: OS Independent",
    ],
)
