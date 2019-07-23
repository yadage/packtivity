import logging
import copy

from ..syncbackends import finalize_inputs, acquire_job_env, packconfig

log = logging.getLogger(__name__)


class DirectJobMakerMixin(object):
    def __init__(self, **kwargs):
        pass

    def render_process(self, spec, state, metadata, local_pars):
        job, env = acquire_job_env(spec, local_pars, state, metadata, packconfig())

        # hacky until we handle logs in more principled way (backend should have log awareness?)
        logdir = state.metadir
        logpath = "{}/{}.{}.log".format(logdir, metadata["name"], "run")

        command = "mkdir -p {logdir}; ({command}) 2>&1 | tee {logpath}"
        script = """mkdir -p {logdir}; cat << 'RECASTJOB' | {} 2>&1 | tee {logpath} \n{}\nRECASTJOB\n"""
        return {
            "command": command.format(
                command=job["command"], logpath=logpath, logdir=logdir
            )
            if "command" in job
            else script.format(
                job["interpreter"], job["script"], logdir=logdir, logpath=logpath
            ),
            "env": env,
        }

    def make_external_job(self, spec, parameters, state, metadata):
        spec = copy.deepcopy(spec)

        parameters, state = finalize_inputs(parameters, state)
        rendered_process = self.render_process(spec, state, metadata, parameters)

        return {
            "sequence_spec": {
                "sequence": ["payload"],
                "payload": {
                    "cmd": ["sh", "-c", rendered_process["command"]],
                    "iscfg": False,
                    "image": ":".join(
                        [
                            rendered_process["env"]["image"],
                            rendered_process["env"]["imagetag"],
                        ]
                    ),
                },
            },
            # ....
            "spec": spec,
            "state": state.json(),
        }
