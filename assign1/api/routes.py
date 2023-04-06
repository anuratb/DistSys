
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
    master = request.get_json().get("master")
    slave = request.get_json().get("slave")
    print(master,slave)
    obj = Counter(master,slave)
    while obj._getLeader() is None :
        continue
        
    #obj.incr()
    #print(f"Output: {obj.cnt}")
    return "Success"
@app.route("/incr",methods=["GET"])
def incr():
    obj.incr()
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

