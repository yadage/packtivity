import os
import errno
import jq
import jsonpointer
import yadageschemas


def handler_decorator():
    handlers = {}

    def decorator(name, implementation="default"):
        def wrap(func):
            handlers.setdefault(name, {})[implementation] = func

        return wrap

    return handlers, decorator


def mkdir_p(path):
    # http://stackoverflow.com/questions/600268/mkdir-p-functionality-in-python
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def leaf_iterator(jsonable):
    if not isinstance(jsonable, (list, dict)):
        yield jsonpointer.JsonPointer(""), jsonable
    else:
        allleafs = jq.jq("leaf_paths").transform(jsonable, multiple_output=True)
        leafpointers = [jsonpointer.JsonPointer.from_parts(x) for x in allleafs]
        for x in leafpointers:
            yield x, x.get(jsonable)


def load_packtivity(
    spec, toplevel=os.getcwd(), schemasource=yadageschemas.schemadir, validate=True
):
    # in case that spec is a json reference string, we will treat it as such
    # if it's just a filename, this should not affect it...
    spec, specopts = (
        spec,
        {
            "toplevel": toplevel,
            "schemadir": schemasource,
            "schema_name": "packtivity/packtivity-schema",
            "load_as_ref": True,
        },
    )

    validopts = {
        "schemadir": schemasource,
        "schema_name": "packtivity/packtivity-schema",
    }

    spec = yadageschemas.load(spec, specopts, validate=validate, validopts=validopts)
    return spec
