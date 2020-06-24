import pytest
import packtivity.syncbackends
from packtivity import datamodel as pdm


def test_build_oneline_job(default_handler_config, basic_localfs_state):
    job = packtivity.syncbackends.build_job(
        {"process_type": "string-interpolated-cmd", "cmd": "hello {one} {two}"},
        pdm.create({"one": "ONE", "two": "TWO"}),
        basic_localfs_state,
        default_handler_config,
    )
    assert "command" in job
    assert job["command"] == "hello ONE TWO"


def test_build_script_job(default_handler_config, basic_localfs_state):
    job = packtivity.syncbackends.build_job(
        {
            "process_type": "interpolated-script-cmd",
            "interpreter": "sh",
            "script": "hello {one} {two}\n echo another line {two}",
        },
        pdm.create({"one": "ONE", "two": "TWO"}),
        basic_localfs_state,
        default_handler_config,
    )
    assert "script" in job
    assert "interpreter" in job
