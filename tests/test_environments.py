from packtivity.handlers.publisher_handlers import handlers
from packtivity import datamodel as pdm

from packtivity.handlers.environment_handlers import handlers
from packtivity.syncbackends import finalize_inputs

import logging


def test_docker_parmounts(tmpdir, basic_localfs_state, docker_env_parmounts):
    state = basic_localfs_state
    environment = docker_env_parmounts.spec["environment"]

    parameters, state = finalize_inputs(
        pdm.create({"outputfile": "{workdir}/hello.txt"}), state
    )
    env = handlers[environment["environment_type"]]["default"](
        environment, parameters, state
    )
    assert env["par_mounts"][0]["mountcontent"] == '"{}"'.format(
        parameters["outputfile"]
    )
