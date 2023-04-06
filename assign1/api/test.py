from pysyncobj import SyncObj, replicated

class Counter(SyncObj):
    def __init__(self):
        super().__init__('localhost:4321', ['localhost:4322'])
        self.counter = 0

    @replicated
    def increment_counter(self):
        self.counter += 1

    def get_counter(self):
        return self.counter


counter = Counter()

print(counter.get_counter())  # Output: 0

counter.increment_counter()
counter.increment_counter()

print(counter.get_counter())  # Output: 2
