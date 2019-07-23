import os
from packtivity.datamodels.purejson import PureJsonModel
from packtivity.typedleafs import TypedLeafs

assert PureJsonModel


def create(data, model=None):
    dmimpl = os.environ.get("PACKTIVITY_DATAMODEL_IMPL", "typedleafs")
    if dmimpl == "typedleafs":
        return TypedLeafs(data, model)
    elif dmimpl == "purejson":
        return PureJsonModel(data, model)
    else:
        raise RuntimeError("unknown implementation")
