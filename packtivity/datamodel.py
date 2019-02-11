from packtivity.typedleafs import TypedLeafs

def create(data, model = None):
    return TypedLeafs(data,model)

def leafs(data):
    for p,v in data.leafs():
        yield p,v