
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

