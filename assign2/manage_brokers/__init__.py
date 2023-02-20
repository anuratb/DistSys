from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import os
import threading
import json

DB_URI = 'postgresql+psycopg2://anurat:abcd@127.0.0.1:5432/anurat'


Load_from_db = False

def create_app(test_config = None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config = True)

    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    if(test_config is None):
        app.config['SECRET_KEY'] = '5791628bb0b13ce0c676dfde280ba245'
        app.config['SQLALCHEMY_DATABASE_URI'] = DB_URI
    else:
        obj = json.load(open(test_config))
        ##print(obj["LOAD_FROM_DB"])
        if(obj["LOAD_FROM_DB"]):
            global Load_from_db
            Load_from_db = True
        app.config['SECRET_KEY'] = obj['SECRET_KEY']
        app.config['SQLALCHEMY_DATABASE_URI'] = obj['DB_URI']
    db = SQLAlchemy(app)
  
    

    return app, db

app, db = create_app('./config.json')


def load_from_db():
    pass
    #print(Queue.queue)


# a simple page that says hello
@app.route('/hello1')
def hello1():
    try:
        # msg = Queue.dequeue('A', 0)
        msg = Queue.enqueue('News', 1, "HELLO")
        msg = "Success"
    except Exception as err:
        return err.args[0]

    return msg

@app.route('/hello2')
def hello2():
    try:
        msg = Queue.dequeue('News', 0)

    except Exception as err:
        return err.args[0]

    return msg

@app.route('/testAddtopic')
def testAddtopic():
    try:
        Queue.createTopic("News")
    except Exception as err:
        return err.args[0]

    return "Success adding topic"+str(Queue.listTopics())

@app.route('/testGetSize')
def testGetSize():
    try:
        ln = Queue.getSize('News',0)
    except Exception as err:
        return err.args[0]

    return "Success "+str(ln)

@app.route('/testc')
def testc():
    try:
        c = Queue.registerConsumer('News')
    except Exception as err:
        return err.args[0]

    return "Success "+str(c)

@app.route('/testp')
def testp():
    try:
        c = Queue.registerProducer('News')
    except Exception as err:
        return err.args[0]

    return "Success "+str(c)

app.app_context().push()

if(Load_from_db):load_from_db()
else:
    Queue.clear()
    db.create_all()
    
from api import routes

 

