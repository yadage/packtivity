from packtivity.handlers.publisher_handlers import handlers
from packtivity import datamodel as pdm

from packtivity.handlers.execution_handlers import (
    command_argv,
    docker_execution_cmdline,
)
from packtivity.syncbackends import ExecutionConfig
import logging


def test_docker_cvmfs(tmpdir, basic_localfs_state, docker_env_resources, monkeypatch):
    state = basic_localfs_state
    log = logging.getLogger("test")
    cmdline = docker_execution_cmdline(
        ExecutionConfig(),
        state,
        log,
        {"name": "myname"},
        race_spec={
            "workdir": None,
            "stdin": None,
            "tty": False,
            "argv": ["echo", "hello", "world"],
            "image": "lukasheinrich/testimage",
            "mounts": [
                {
                    "type": "bind",
                    "source": "/cvmfs",
                    "destination": "/cvmfs",
                    "readonly": False,
                }
            ],
        },
    )
    assert "-v /cvmfs:/cvmfs" in cmdline


def test_docker_auth(tmpdir, basic_localfs_state):
    state = basic_localfs_state
    log = logging.getLogger("test")
    cmdline = docker_execution_cmdline(
        ExecutionConfig(),
        state,
        log,
        {"name": "myname"},
        race_spec={
            "workdir": None,
            "stdin": None,
            "tty": False,
            "argv": ["echo", "hello", "world"],
            "image": "lukasheinrich/testimage",
            "mounts": [
                {
                    "type": "bind",
                    "source": "/home/recast/recast_auth",
                    "destination": "/recast_auth",
                    "readonly": False,
                }
            ],
        },
    )
    assert "-v /home/recast/recast_auth:/recast_auth:rw" in cmdline
