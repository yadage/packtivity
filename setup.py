from setuptools import setup, find_packages
from pathlib import Path

deps = [
    "requests[security]",
    "jsonschema",
    "jsonref",
    "pyyaml",
    "click",
    "glob2",
    "jsonpointer",
    "jsonpath-rw",
    "jq>=1.0.0",
    "yadage-schemas",
    "mock",
    "dirhash>=0.4.0",
]

this_directory = Path(__file__).parent

setup(
    name="packtivity",
    version="0.17.0",
    description="packtivity - general purpose schema + bindings for PROV activities",
    long_description=(this_directory / "README.md").read_text(),
    long_description_content_type="text/markdown",
    url="https://github.com/yadage/packtivity",
    author = 'Lukas Heinrich, Matthew Feickert',
    author_email = 'lukas.heinrich@gmail.com, matthew.feickert@cern.ch',
    license="MIT",
    packages=find_packages(),
    python_requires=">=3.8",
    include_package_data=True,
    install_requires=deps,
    extras_require={
        "celery": [
            "celery>=5.0.0",
            "redis",
        ]
    },
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
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: Implementation :: CPython",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Physics",
        "Operating System :: OS Independent",
    ],
)
