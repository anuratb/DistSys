from pysyncobj import SyncObj,replicated

class Counter(SyncObj):
    
    
    def __init__(self, selfNodeAddr = None, partnerNodeAddrs = None):
        super(Counter, self).__init__(selfNodeAddr, partnerNodeAddrs)
        self.cnt = 0
    
    @replicated
    def incr(self):
        self.cnt+=1

obj = None

def setObj(selfAddr, otherAddrs):
    global obj
    obj = Counter(selfAddr, otherAddrs)

def getObj():
    return obj