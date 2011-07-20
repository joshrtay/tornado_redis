class Jso(dict):
    def __init__(self,*args):
        assert(len(args) < 2)
        dic = args[0] if args else {}
        for k,v in dic.iteritems():
            self[k] = v

    def __getattr__(self,value):
        return self[value] if value in self else None
            
    __setattr__= dict.__setitem__
    __delattr__= dict.__delitem__
