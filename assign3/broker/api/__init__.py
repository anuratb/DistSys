from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import os
import threading
import json

DB_URI = 'postgresql+psycopg2://anurat:abcd@10.102.68.15:5432/anurat'




def create_app(test_config = None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config = True)

    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    secret_key = '5791628bb0b13ce0c676dfde280ba245'
    if(test_config is not None):        
    
        obj = json.load(open(test_config))
        ##print(obj["LOAD_FROM_DB"])
     
        secret_key = obj['SECRET_KEY']
        global DB_URI
        DB_URI = obj['DB_URI']
    if ('DB_URI' in os.environ.keys()):        
        DB_URI = os.environ['DB_URI']
    app.config['SECRET_KEY'] = secret_key
    app.config['SQLALCHEMY_DATABASE_URI'] = DB_URI
    db = SQLAlchemy(app)
    '''
    #TODO testing and create database
    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass
    '''
    

    return app, db

app, db = create_app('./config.json')

from api.data_struct import TopicNode,Queue
from api.models import QueueDB,Topics,Producer,Consumer

def load_from_db():
    #print('Loading from db')
    Queue.clear()
    Queue.glob_lck = threading.Lock()
    for topic in Topics.query.all():
        Queue.topics[topic.value] = TopicNode(topic.id)
        Queue.queue[topic.id] = []
        Queue.locks[topic.id] = threading.Lock()
        cur = topic.start_ind
        lst = topic.end_ind
        #Construct the queue for the given topic
        while(cur is not None):
            obj = QueueDB.query.filter_by(id=cur).first()
            Queue.queue[topic.id].append([obj.id,obj.value])
            if(cur==lst):break
            cur = obj.nxt_id
        #construct the producers
        for producer in topic.producers:
            Queue.topics[topic.value].subscribeProducer(producer.id)
        #construct the consumers
        for consumer in topic.consumers:
            Queue.topics[topic.value].subscribeConsumer(consumer.id)
            Queue.consumers[consumer.id] = [consumer.offset,threading.Lock()]
    Queue.cntProd = Producer.query.count()
    Queue.cntCons = Consumer.query.count()
    Queue.cntMessage = QueueDB.query.count()
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

Queue.clear()
# db.drop_all()
db.create_all()
load_from_db()

    
    
    
from api import routes

 

