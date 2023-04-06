from flask import Flask
from pysyncobj import SyncObj, replicated
import threading, time

app = Flask(__name__)

lock = threading.Lock()

class User(SyncObj):

    def __init__(self, selfNodeAddr, otherNodeAddrs):
        super(User, self).__init__(selfNodeAddr, otherNodeAddrs)
        # UserName -> List of Accounts
        self.counter = 0
        self.lock = threading.Lock()
        
    @replicated
    def incCounter(self, val):
        lock.acquire()
        self.counter += val

    
    def getCounter(self):
        return self.counter

user = User("localhost:6002", ["localhost:6003"])

@app.route("/")
def hello_world():
    return str(user.getCounter())

@app.route("/inc")
def hell():
    user.incCounter(10, sync=True)
    return "Done"