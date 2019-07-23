import os
import importlib
import logging
from .posixfs_context import LocalFSState

from ..utils import handler_decorator

log = logging.getLogger(__name__)

statehandlers, stateloader = handler_decorator()


@stateloader("frompython")
def frompython_stateloader(jsondata, **opts):
    statestring = opts["statestring"]
    _, module, stateclass = statestring.split(":")
    module = importlib.import_module(module)
    stateclass = getattr(module, stateclass)
    stateopts = {}
    return stateclass.fromJSON(jsondata, **stateopts)


@stateloader("localfs")
def localfs_stateloader(jsondata, **opts):
    return LocalFSState.fromJSON(jsondata)


@stateloader("fromenv")
def fromenv_stateloader(jsondata, **opts):
    module = importlib.import_module(os.environ["PACKTIVITY_STATEPROVIDER"])
    return module.load_state(jsondata)


def load_state(jsondata, deserialization_opts=None):
    log.debug("load_state opts %s", deserialization_opts)
    deserialization_opts = deserialization_opts or {}
    if "state" in deserialization_opts:
        statestring = deserialization_opts.get("state", "")
        if statestring.startswith("py:"):
            return statehandlers["frompython"]["default"](
                jsondata, statestring=statestring
            )

    if "PACKTIVITY_STATEPROVIDER" in os.environ:
        return statehandlers["fromenv"]["default"](jsondata)

    if jsondata["state_type"] in statehandlers:
        return statehandlers[jsondata["state_type"]]["default"](jsondata)

    raise TypeError("unknown state type {}".format(jsondata["state_type"]))
