from pysyncobj import SyncObj,replicated

class Counter(SyncObj):
    def __init__(self):
        super().__init__('127.0.0.1:9090', ['127.0.0.1:9091'])
        self.value = 0
    @replicated 
    def increment(self):
        self.value += 1
       # self.sync()
        
    def get_value(self):
        return self.value
if __name__=='__main__':
    counter = Counter()
    counter.increment()
    print(counter.get_value())
    counter.increment()
    print(counter.get_value())