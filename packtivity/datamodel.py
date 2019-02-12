from packtivity.datamodels.purejson import PureJsonModel
from packtivity.typedleafs import TypedLeafs

def create(data, model = None):
    return TypedLeafs(data,model)
    # return PureJsonModel(data,model)

