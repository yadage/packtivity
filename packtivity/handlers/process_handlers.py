import click
import yaml

import packtivity.utils as utils

handlers, process = utils.handler_decorator()


@process("string-interpolated-cmd")
def stringinterp_handler(process_spec, parameters, state):
    if isinstance(parameters.typed(), dict):
        flattened_kwargs = {
            k: v if not (type(v) == list) else " ".join([str(x) for x in v])
            for k, v in list(parameters.typed().items())
        }
        command = process_spec["cmd"].format(**flattened_kwargs)
    elif isinstance(parameters.typed(), list):
        flattened_args = [
            v if not (type(v) == list) else " ".join([str(x) for x in v])
            for v in parameters.typed()
        ]
        command = process_spec["cmd"].format(*flattened_args)
    else:
        command = process_spec["cmd"].format(value=parameters.typed())

    return {"command": command}


@process("interpolated-script-cmd")
def interp_script(process_spec, parameters, state):
    if isinstance(parameters.typed(), dict):
        flattened_kwargs = {
            k: v if not (type(v) == list) else " ".join([str(x) for x in v])
            for k, v in list(parameters.typed().items())
        }
        script = process_spec["script"].format(**flattened_kwargs)
    elif isinstance(parameters.typed(), list):
        flattened_args = [
            v if not (type(v) == list) else " ".join([str(x) for x in v])
            for v in parameters.typed()
        ]
        script = process_spec["script"].format(*flattened_args)
    else:
        script = process_spec["script"].format(value=parameters.typed())
    return {"script": script, "interpreter": process_spec["interpreter"]}


@process("manual-instructions-proc")
def manual_proc(process_spec, parameters, state):
    instructions = process_spec["instructions"]
    attrs = yaml.safe_dump(parameters, default_flow_style=False)
    click.secho(instructions, fg="blue")
    click.secho(attrs, fg="cyan")


@process("test-process")
def test_process(process_spec, parameters, state):
    return {"a": "complicated", "job": "with", "pars": parameters}
