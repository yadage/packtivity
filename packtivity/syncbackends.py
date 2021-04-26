import yaml
import os

import packtivity.logutils as logutils
from . import datamodel as _datamodel


class packconfig(object):
    def __init__(self, **kwargs):
        self.handler_selection = kwargs
        fromenv = os.environ.get("PACKTIVITY_SYNCCONFIGFILE", None)
        if fromenv:
            override = yaml.safe_load(open(fromenv))
            self.handler_selection.update(**override)

    def get_impl(self, category, handler):
        try:
            return self.handler_selection[category][handler]
        except KeyError:
            return "default"


class container_config(object):
    def __init__(self, config=None):
        self.config = config or {}

    def workdir_location(self):
        return os.environ.get("PACKTIVITY_WORKDIR_LOCATION")

    def pull_software(self):
        if "PACKTIVITY_DOCKER_NOPULL" in os.environ:
            return False
        return self.config.get("pull_images", True)

    def container_runtime(self):
        return os.environ.get("PACKTIVITY_CONTAINER_RUNTIME", "docker")

    def cvmfs_repos(self):
        cvmfs_repos = yaml.safe_load(os.environ.get("PACKTIVITY_CVMFS_REPOS", "null"))
        if not cvmfs_repos:
            cvmfs_repos = ["atlas.cern.ch", "atlas-condb.cern.ch", "sft.cern.ch"]
        return cvmfs_repos

    def cvmfs_location(self):
        return os.environ.get("PACKTIVITY_CVMFS_LOCATION", "/cvmfs")

    def cvmfs_propagation(self):
        return os.environ.get("PACKTIVITY_CVMFS_PROPAGATION")

    def cvmfs_source(self):
        return os.environ.get("PACKTIVITY_CVMFS_SOURCE", "external")

    def container_runtime_modifier(self):
        return os.environ.get("PACKTIVITY_DOCKER_CMD_MOD", "")

    def auth_location(self):
        env_or_default = os.environ.get(
            "PACKTIVITY_AUTH_LOCATION", "/home/recast/recast_auth"
        )
        return self.config.get("auth_location", env_or_default)

    def auth_targetdir(self):
        env_or_default = os.environ.get(
            "PACKTIVITY_AUTH_TARGETDIR", "/recast_auth"
        )
        return self.config.get("auth_targetdir", env_or_default)

class ExecutionConfig(object):
    def __init__(self, config=None):
        self.config = config or {}
        self.container_config = container_config(self.config.get("containers"))

    def disable_logging(self):
        if "PACKTIVITY_LOGGING_DISABLE" in os.environ:
            return yaml.safe_load(os.environ.get("PACKTIVITY_LOGGING_DISABLE", "false"))
        return not self.config.get("logging", True)

    def custom_logging_handler(self):
        return os.environ.get("PACKTIVITY_LOGGING_HANDLER")

    def stream_loglevel(self):
        if "PACKTIVITY_LOGGING_STREAM_LEVEL" in os.environ:
            return os.environ.get("PACKTIVITY_LOGGING_STREAM_LEVEL", "INFO")
        return self.config.get("logging_level", "INFO")

    def dry_run(self):
        if "PACKTIVITY_DRYRUN" in os.environ:
            return os.environ["PACKTIVITY_DRYRUN"]
        return self.config.get("dry_run", False)


def build_job(process, parameters, state, pack_config):
    """
    takes a process template and builds a job out of it using a handler.
    """
    proc_type = process["process_type"]
    impl = pack_config.get_impl("process", proc_type)
    from .handlers.process_handlers import handlers as proc_handlers

    handler = proc_handlers[proc_type][impl]
    return handler(process, parameters, state)


def build_env(environment, parameters, state, pack_config):
    """
    builds an environment template description and builds a fully-defined env
    this will use a handler in the future (just as build_job)
    """

    env_type = environment["environment_type"]
    impl = pack_config.get_impl("environment", env_type)
    from .handlers.environment_handlers import handlers as env_handlers

    try:
        handler = env_handlers[env_type][impl]
    except KeyError:
        handler = env_handlers["default"]["default"]
    return handler(environment, parameters, state)


def run_in_env(job, environment, state, metadata, pack_config, exec_config):
    """
    takes a job and an environment and executes with the state context attached
    """
    env_type = environment["environment_type"]
    impl = pack_config.get_impl("executor", env_type)
    from .handlers.execution_handlers import handlers as exec_handlers

    handler = exec_handlers[env_type][impl]
    return handler(exec_config, environment, state, job, metadata)


def publish(publisher, parameters, state, pack_config, datamodel=_datamodel):
    pub_type = publisher["publisher_type"]
    impl = pack_config.get_impl("publisher", pub_type)
    from .handlers.publisher_handlers import handlers as pub_handlers

    handler = pub_handlers[pub_type][impl]
    pubdata = handler(publisher, parameters, state)
    return datamodel.create(pubdata, state.datamodel if state else None)


def finalize_inputs(parameters, state, datamodel=_datamodel):
    parameters = datamodel.create(parameters, state.datamodel if state else None)
    if not state:
        return parameters, state
    return state.model(parameters), state


def finalize_outputs(pubdata):
    return pubdata


def prepublish(spec, parameters, state, pack_config):
    """
    attempts to prepublish output data, returns None if not possible
    """
    parameters, state = finalize_inputs(parameters, state)
    pub = spec["publisher"]

    pubdata = None
    if pub["publisher_type"] in ["frompar-pub", "constant-pub"]:
        return publish(pub, parameters, state, pack_config)
    if pub["publisher_type"] in ["interpolated-pub", "fromparjq-pub"]:
        from .statecontexts.posixfs_context import LocalFSState

        if not state:
            return publish(pub, parameters, state, pack_config)
        if type(state) == LocalFSState:
            if pub["glob"] == False or len(state.readwrite) == 0:
                pubdata = publish(pub, parameters, state, pack_config)
    return pubdata


def acquire_job_env(spec, parameters, state, metadata, config):
    if spec["process"] and spec["environment"]:
        job = build_job(spec["process"], parameters, state, config)
        env = build_env(spec["environment"], parameters, state, config)
        return job, env
    return None, None


def run_packtivity(spec, parameters, state, metadata, pack_config, exec_config):
    with logutils.setup_logging_topic(
        exec_config, metadata, state, "step", return_logger=True
    ) as log:
        parameters, state = finalize_inputs(parameters, state)
        job, env = acquire_job_env(spec, parameters, state, metadata, pack_config)

        if job and env:
            try:
                run_in_env(job, env, state, metadata, pack_config, exec_config)
            except:
                log.exception(
                    "job execution if job %s raise exception exception", metadata
                )
                raise

        pubdata = publish(spec["publisher"], parameters, state, pack_config)
        pubdata = finalize_outputs(pubdata)
        log.info("publishing data: %s", pubdata)
        return pubdata


class defaultsyncbackend(object):
    def __init__(self, config=None):
        config = config or {}
        self.exec_config = ExecutionConfig(config.pop("exec", None))
        self.pack_config = packconfig(**config) if config else packconfig()

    def prepublish(self, spec, parameters, state):
        return prepublish(spec, parameters, state, self.pack_config)

    def run(self, spec, parameters, state, metadata={"name": "packtivity_syncbackend"}):
        return run_packtivity(
            spec, parameters, state, metadata, self.pack_config, self.exec_config
        )
