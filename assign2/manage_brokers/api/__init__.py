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

    db = None
    # db = SQLAlchemy(app)
    return app, db

app, db = create_app()


def load_from_db():
    pass
    #print(Queue.queue)


# a simple page that says hello
@app.route('/hello1')
def hello1():
    return "Hello"

app.app_context().push()

if(Load_from_db):load_from_db()
else:
    pass
    # db.create_all()
    
from . import routes

 

