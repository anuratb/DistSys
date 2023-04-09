

from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import os
from data_struct import setObj
DB_URI = 'postgresql+psycopg2://anurat:abcd@127.0.0.1:5432/anurat'
cnt = 0
setObj("localhost:6125", ["localhost:6124"])
app = Flask(__name__)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SECRET_KEY'] = '5791628bb0b13ce0c676dfde280ba245'
app.config['SQLALCHEMY_DATABASE_URI'] = DB_URI
db = SQLAlchemy(app)


from data_struct import getObj
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

import time


# @app.route("/counter",methods=["GET"])
# def counter():
#     global obj
#     master = request.args.get('master', type=str)
#     slave = [request.args.get('slave', type=str)]
#     print(master, slave)
#     obj = Counter(master,slave)
#     # obj.__init__(master, slave)
#     while True:
#         time.sleep(1)
#         L = obj._getLeader()
#         if L is not None:
#             print(L)
#             break

#     return "Success"
    
@app.route("/incr",methods=["GET"])
def incr():
    # while obj._getLeader() is None :
    #     time.sleep(2)
    #     print("Getting Leader")
    #     continue
    # print(f"Leader {obj._getLeader()}")
    print("INCR:")
    getObj().incr(sync = True)
    return "Success"

@app.route("/getval",methods=["GET"])
def getval():
    return str(getObj().cnt)


# @ app.route("/topics", methods=[ 'GET','POST'])
# def topics():
#     print('Hello')
#     global cnt
  
#    # print(flask.request.get_json())
#     if(flask.request.method=='GET'):
#         cnt+=1
#         print("---->"+str(cnt))
#         ret = "Hello"
#         return ret
#     else:
#         pass



# @ app.route("/topics2", methods=[ 'GET','POST'])
# def topics2():
#     global cnt
#     if(flask.request.method=='GET'):
#         cnt+=1
#         print(cnt)
#         ret = "Hello"
#         return ret
#     else:
#         pass





