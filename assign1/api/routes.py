
from flask import   request, jsonify
from api import app,db,cnt
import flask

'''
Method: POST
Endpoint: /topics
Params:
- "name": <string>
Response:
onSuccess:
- "status": “success”
- "message": <string>
onFailure:
- "status": “failure”
- "message": <string> // informative error message


Method: GET
Endpoint: /topics
Params:
None
Response:
"status": <string>
onSuccess:
- "topics": List[<string>] // List of topic names
onFailure:
- "message": <string> // Error message
'''
from pysyncobj import SyncObj,replicated
import time
class Counter(SyncObj):
    
    
    def __init__(self, selfNodeAddr = None, partnerNodeAddrs = None):
        super(Counter, self).__init__(selfNodeAddr, partnerNodeAddrs)
        self.cnt = 0
    
    @replicated
    def incr(self):
        self.cnt+=1

obj = None

@app.route("/counter",methods=["GET"])
def counter():
    global obj
    master = request.args.get('master', type=str)
    slave = [request.args.get('slave', type=str)]
    print(master, slave)
    obj = Counter(master,slave)
    # obj.__init__(master, slave)
    while True:
        time.sleep(1)
        L = obj._getLeader()
        if L is not None:
            print(L)
            break

    return "Success"
    
@app.route("/incr",methods=["GET"])
def incr():
    # while obj._getLeader() is None :
    #     time.sleep(2)
    #     print("Getting Leader")
    #     continue
    # print(f"Leader {obj._getLeader()}")
    obj.incr(sync = True)
    return "Success"

@app.route("/getval",methods=["GET"])
def getval():
    return str(obj.cnt)


@ app.route("/topics", methods=[ 'GET','POST'])
def topics():
    print('Hello')
    global cnt
  
   # print(flask.request.get_json())
    if(flask.request.method=='GET'):
        cnt+=1
        print("---->"+str(cnt))
        ret = "Hello"
        return ret
    else:
        pass



@ app.route("/topics2", methods=[ 'GET','POST'])
def topics2():
    global cnt
    if(flask.request.method=='GET'):
        cnt+=1
        print(cnt)
        ret = "Hello"
        return ret
    else:
        pass

