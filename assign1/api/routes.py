
from flask import request, jsonify
from api import app, db

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
    if(flask.request.method=='GET'):
        pass
    else:
        pass




