

from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import os
import threading

DB_URI = 'postgresql+psycopg2://anurat:abcd@127.0.0.1:5432/anurat'

class TopicNode:
    
    def __init__(self, topicID_):
        self.topicID = topicID_
        self.producerList = [] # List of subscribed producers
        self.consumerList = [] # List of subscribed consumers

    def subscribeProducer(self, producerID_):
        self.producerList.append(producerID_)
    
    def subscribeConsumer(self, consumerID_):
        self.consumerList.append(consumerID_)


class Queue:
    # topic-wise message queues
    # Key: TopicID, Value: An array of messages
    queue = {}
    # Topic-wise locks
    # Key: TopicID, Value: lock
    locks = {}
    # Key: topicname, Value: TopicNode
    topics = {}
    # Key: Consumer ID, Value: offset in the topic queue
    consumers = {}

    @classmethod
    def createTopic(topicName):
        pass

    @classmethod
    def listTopics():
        pass

    @classmethod
    def registerConsumer(topicName):
        pass

    @classmethod
    def registerProducer(topicName):
        pass

    @classmethod
    def enqueue(topicName, prodID, msg):
        pass

    @classmethod
    def dequeue(topicName, conID):
        pass
    
    @classmethod
    def getSize(topicName, conID):
        pass

def create_app(test_config = None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config = True)

    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['SECRET_KEY'] = '5791628bb0b13ce0c676dfde280ba245'
    app.config['SQLALCHEMY_DATABASE_URI'] = DB_URI
    db = SQLAlchemy(app)
    
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
    
    Queue.locks = {0: threading.Lock(), 1: threading.Lock()}

    # a simple page that says hello
    @app.route('/hello')
    def hello():
        Queue.locks[0].acquire()
        return 'Hello, World!'

    return app

 

