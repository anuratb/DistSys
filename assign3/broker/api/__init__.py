from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import os
import threading, time
import json
import dotenv

DB_URI = 'postgresql+psycopg2://anurat:abcd@10.102.68.15:5432/anurat'

#dotenv_file = dotenv.find_dotenv()
#dotenv.load_dotenv(dotenv_file)


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
from api.data_struct import createQObj
#print(os.environ)
print(os.environ["SLAVE_ADDR"],os.environ["SLAVE_ADDR"].split('$'))
createQObj(os.environ["SELF_ADDR"],os.environ["SLAVE_ADDR"].split('$'))
from api.data_struct import QObj
while QObj._getLeader() is None:
    time.sleep(1)
    print("No leader")
    continue
print("Leader Election Success")
master_ip,master_port = os.environ["SELF_ADDR"].split(':')
master_port = int(master_port)+1
slave = os.environ["SLAVE_ADDR"].split('$')
print(slave,os.environ["SLAVE_ADDR"])
slave = [itr.split(':') for itr in slave]
slave_ip = [itr[0] for itr in slave]
slave_port = [int(itr[1])+1 for itr in slave]
# createSyncObj(str(master_ip)+':'+str(master_port),[str(itr[0])+':'+str(itr[1]) for itr in zip(slave_ip,slave_port)])
# from api.data_struct import syncObj
# while syncObj._getLeader() is None:
#     continue

print("HOOOOOOOOOOOOOO")
#from api.data_struct import QueueList

from api.models import QueueDB,Topics,Producer,Consumer
'''
def load_from_db():
    conID = set()
    for cons in Consumer.query.all():
        conID.add(cons.id)
    conID = list(conID)
    for id in conID:
        QObj.offsetLock[conID] = threading.Lock()
    for topic in Topics.query.all():
        QObj.QList[topic.value] = Queue(topic.id,topic.value)
        QObj.QLock[topic.value] = threading.Lock()
        
        #Queue.topics[topic.value] = TopicNode(topic.id)
        #Queue.queue[topic.id] = []
        #Queue.locks[topic.id] = threading.Lock()
        cur = topic.start_ind
        lst = topic.end_ind
        #Construct the queue for the given topic
        while(cur is not None):
            obj = QueueDB.query.filter_by(id=cur).first()
            #TODO
           # Queue.queue[topic.id].append([obj.id,obj.value])
            if(cur==lst):break
            cur = obj.nxt_id
        #construct the producers
        for producer in topic.producers:
            pass
            #Queue.topics[topic.value].subscribeProducer(producer.id)
        #construct the consumers
        for consumer in topic.consumers:
            pass
            #Queue.topics[topic.value].subscribeConsumer(consumer.id)
            #Queue.consumers[consumer.id] = [consumer.offset,threading.Lock()]
    #Queue.cntProd = Producer.query.count()
    #Queue.cntCons = Consumer.query.count()
    #Queue.cntMessage = QueueDB.query.count()
    #print(Queue.queue)

    return
'''


'''
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
    '''

print("HOOOOOOOOOOOOO46356346699999999")
app.app_context().push()
print("HOOOOOOOOOOOOO46356346677777777")
#from api.data_struct import Queue
#Queue.clear()
#if int(os.environ.get("CLEAR_DB"))==1:
#    db.drop_all()
print("HOOOOOOOOOOOOO#4%%%%%%%")
db.create_all()
#load_from_db()

print("HOOOOOOOOOOOOO#435424")
    
    
from api import routes

 

