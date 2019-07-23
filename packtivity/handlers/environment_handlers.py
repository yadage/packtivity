import packtivity.utils as utils
import jq
import copy

handlers, environment = utils.handler_decorator()


@environment("docker-encapsulated")
def docker(environment, parameters, state):
    environment = copy.deepcopy(environment)

    jsonpars = parameters.json()
    for p, v in parameters.leafs():
        if p.path == "":
            jsonpars = v
            break
        p.set(jsonpars, v)

    for i, x in enumerate(environment["par_mounts"]):
        script = x.pop("jqscript")
        x["mountcontent"] = jq.jq(script).transform(jsonpars, text_output=True)

    if environment["workdir"] is not None:
        environment["workdir"] = state.contextualize_value(environment["workdir"])
    return environment


@environment("default")
def default(environment, parameters, state):
    return environment
