from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import os
import threading
import json
import dotenv
from dotenv import load_dotenv
import psycopg2
from random import random
from sqlalchemy_utils.functions import database_exists
from api.utils import *
from flask_executor import Executor
from apscheduler.schedulers.background import BackgroundScheduler


dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)

APP_URL = "http://127.0.0.1:5124"


if 'IS_WRITE' not in os.environ.keys():
    os.environ['IS_WRITE'] = '1'




Load_from_db = os.environ["LOAD_FROM_DB"]=='True'

DB_URI = 'postgresql+psycopg2://anurat:abcd@127.0.0.1:5432/anurat'
DOCKER_DB_URI = 'postgresql+psycopg2://anurat:abcd@127.0.0.1:5432/'
LOG_path = "./LOG.txt"

db_username = os.environ['DB_USERNAME']

db_password = os.environ['DB_PASSWORD']

db_host = os.environ['DB_HOST']

db_port = os.environ['DB_PORT']


docker_img_broker = os.environ['DOCKER_IMG_BROKER']

postgres_container = os.environ['POSTGRES_CONTAINER']
print(docker_img_broker,db_username,db_host)


###############################GLOBALS#####################################
#random.seed(int(os.environ['RANDOM_SEED']))
globLock = threading.Lock()
#global cntManager 
cntManager = 0 
# cntManager = len(ManagerDB.query.all())
readManagerURL = []

replFactor = int(os.environ['REPLICATION_FACTOR'])

###########################################################################


def create_app(test_config = None):
    # create and configure the app
    global DOCKER_DB_URI
    app = Flask(__name__, instance_relative_config = True)
    global DB_URI
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['SECRET_KEY'] = '5791628bb0b13ce0c676dfde280ba245'
    app.config['SQLALCHEMY_DATABASE_URI'] = DB_URI
    
    if ('DOCKER_DB_URI' in os.environ.keys()):        
        DOCKER_DB_URI = os.environ['DOCKER_DB_URI']
    
    #obj = json.loads(os.popen("docker inspect '{}'".format(postgres_container)).read())
   
    DOCKER_DB_URI = 'postgresql+psycopg2://'+db_username+':'+db_password+'@'+db_host+':'+str(db_port)+'/'
    
    DB_URI = DOCKER_DB_URI + os.environ['DB_NAME']
    app.config['SQLALCHEMY_DATABASE_URI'] = DB_URI
    db = SQLAlchemy(app)
    return app, db

app, db = create_app()




from api.data_struct import setManager, getManager, is_leader

ip_list1 = [2*x+2 for x in range(int(os.environ["NUMBER_OF_BROKERS"]))]
ip_list2 = [x+2*len(ip_list1)+5 for x in range(int(os.environ["NUMBER_READ_MANAGERS"]))]
post_ip = [f"172.18.0.{x+1}" for x in ip_list1]
ip_list1 = [f"172.18.0.{x}" for x in ip_list1]

ip_list2 = [f"172.18.0.{x}" for x in ip_list2]

# TODO get addrs
raft_ip = os.environ.get("raft_ip").split("$")
self_index = int(os.environ.get("CUR_ID"))
selfAddr = raft_ip[self_index]
manager_ip = os.environ.get("READ_MANAGER_URLS").split("$")
otherAddrs = [x for x in raft_ip if x != selfAddr]
for ip in manager_ip:
    readManagerURL.append(ip)

IsWriteManager = (int(os.environ["IS_WRITE"])==1)

setManager(selfAddr, otherAddrs)

while getManager()._getLeader() is None:
    time.sleep(1)
    print("Electing Leader...")

# dotenv_file = dotenv.find_dotenv()
# dotenv.load_dotenv(dotenv_file)

if 'IS_WRITE' not in os.environ.keys():
    os.environ['IS_WRITE'] = '1'




Load_from_db = os.environ["LOAD_FROM_DB"]=='True'
print(Load_from_db)

# def create_app(test_config = None):
#     # create and configure the app
#     global DOCKER_DB_URI
#     app = Flask(__name__, instance_relative_config = True)
#     global DB_URI
#     app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
#     app.config['SECRET_KEY'] = '5791628bb0b13ce0c676dfde280ba245'
#     app.config['SQLALCHEMY_DATABASE_URI'] = DB_URI
    
#     if ('DOCKER_DB_URI' in os.environ.keys()):        
#         DOCKER_DB_URI = os.environ['DOCKER_DB_URI']
    
#     #obj = json.loads(os.popen("docker inspect '{}'".format(postgres_container)).read())
   
#     DOCKER_DB_URI = 'postgresql+psycopg2://'+db_username+':'+db_password+'@'+db_host+':'+str(db_port)+'/'
    
#     DB_URI = DOCKER_DB_URI + os.environ['DB_NAME']
#     app.config['SQLALCHEMY_DATABASE_URI'] = DB_URI
#     db = SQLAlchemy(app)
#     return app, db

# app, db = create_app()

from api.models import ManagerDB,TopicDB,TopicBroker,BrokerMetaDataDB,globalProducerDB,globalConsumerDB,DockerDB,localProducerDB,localConsumerDB

cntManager = 0
def create_read_manager(read_urls):
    # global cntManager
    # manager_nme = "manager"+str(cntManager)
    # #db_uri = create_postgres_db(manager_nme,manager_nme+"_db",db_username,db_password)    
    # db_uri = DB_URI #sharing database 
    # url = create_container(db_uri,manager_nme,os.environ['DOCKER_IMG_MANAGER'],envs={
    #     'IS_WRITE':'0'
    # })      
    # print(url)
    
    ################# DB UPDATES ########################
    
    db.session.add(ManagerDB(id=manager_nme,url=url))
    db.session.commit()
    #####################################################

    globLock.acquire()
    cntManager+=1
    readManagerURL.append(url)
    globLock.release()
    return url
from api.data_struct import  Manager,BrokerMetaData
from api.models import DockerDB
# TODO Modify DB updates below 
'''
def load_from_db():

    ############################ Write Pending commits to DB ######################################
    #if not os.path.exists(WAL_path):
    #    obj = open(WAL_path,"w")
    #    obj.close()
    #walFile = open(WAL_path, "r+")
    #for line in walFile.readlines():
    #    exec(line)
    #db.session.commit()
    #erase the file contents
    #walFile.truncate(0)
    #walFile.close()
    ###############################################################################################
    db.session.rollback()



    ################################### Load Topic MetaData  ########################################
    topicMetaData = TopicMetaData()
    for topic in TopicDB.query.all():
        topicMetaData.Topics[topic.topicName] = [topic.topic_id,topic.numPartitions,topic.rrindex]
    for topic in TopicBroker.query.all():
        topicMetaData.PartitionBroker[str(topic.partition)+"#"+topic.topic] = topic.brokerID
    topicMetaData.lock = threading.Lock()
    #for broker in BrokerMetaDataDB.query.all():
    #    topicMetaData.BrokerUrls.insert(broker.url)
        #BrokerMetaData.brokers[broker.id] = broker.url
    ###############################################################################################

    ################################### Load Producers  ########################################
    producers = ProducerMetaData(len(globalProducerDB.query.all()))
    producers.subscriptionLock = threading.Lock()
    for producer in globalProducerDB.query.all():
        producers.subscription[str(producer.glob_id)+"#"+producer.topic] = []
        producers.rrIndex[str(producer.glob_id)+"#"+producer.topic] = producer.rrindex
        producers.rrIndexLock[str(producer.glob_id)+"#"+producer.topic] = threading.Lock()

        agg = {}
        for local in producer.localProducer:
            if(local.broker_id not in agg.keys()):
                agg[local.broker_id] = []
            agg[local.broker_id].append([local.local_id,local.partition])
        for brokerId,local_ids in agg.items():
            producers.subscription[str(producer.glob_id)+"#"+producer.topic].append([brokerId,*local_ids])
    ###########################################################################################
    


    ################################### Load Consumers  ########################################
    consumers = ConsumerMetaData(len(globalConsumerDB.query.all()))
    consumers.subscriptionLock = threading.Lock()
    for consumer in globalConsumerDB.query.all():
        consumers.subscription[str(consumer.glob_id)+"#"+consumer.topic] = []
        consumers.rrIndex[str(consumer.glob_id)+"#"+consumer.topic] = consumer.rrindex
        consumers.rrIndexLock[str(consumer.glob_id)+"#"+consumer.topic] = threading.Lock()

        agg = {}
        for local in consumer.localConsumer:
            if(local.broker_id not in agg.keys()):
                agg[local.broker_id] = []
            agg[local.broker_id].append([local.local_id,local.partition])
        for brokerId,local_ids in agg.items():
            consumers.subscription[str(consumer.glob_id)+"#"+consumer.topic].append([brokerId,*local_ids])
    ###########################################################################################

    ################################### Create Manager  ########################################
    Manager.topicMetaData = topicMetaData
    Manager.producerMetaData = producers
    Manager.consumerMetaData = consumers
    Manager.lock = threading.Lock()
    
    ###########################################################################################


    ################################### Load Broker MetaData  #######################################
    #global brokersDocker
    #brokersDocker = Docker()
    Manager.brokers = {}
    for broker in BrokerMetaDataDB.query.all():
        curr = BrokerMetaData(
            broker.db_uri,
            broker.url,
            broker.docker_name,
            broker.broker_id,
            broker.docker_id

        )
        Manager.brokers[broker.broker_id] = curr
    count = len(BrokerMetaDataDB.query.all())
    Docker.cnt = len(BrokerMetaDataDB.query.all())
    nw = Docker.cnt
        #brokersDocker.brokers[broker.docker_name] = curr
    ###########################################################################################
    if IsWriteManager:
        cntManager = len(ManagerDB.query.all())
        for obj in ManagerDB.query.all():
            readManagerURL.append(obj.url)

'''

app.app_context().push()

# TODO Modify this
if (IsWriteManager):
    if(Load_from_db):
        #load_from_db()
        pass
    else:
        #db.session.remove()
        db.drop_all()
        db.create_all()
        db.session.commit()
        db.session.add(DockerDB(id=0))
        db.session.commit()


# if os.environ['EXECUTE'] == '0':
#     os.environ['EXECUTE'] = '1'
if is_leader() and not Load_from_db:
    # for _ in range(int(os.environ["NUMBER_READ_MANAGERS"])):
    #     create_read_manager()

    # for _ in range(int(os.environ["NUMBER_READ_MANAGERS"])):
    #     create_read_manager()
    for i in range(int(os.environ["NUMBER_OF_BROKERS"])):
        os.system(f"docker rm -f broker{i}")
    for i in range(int(os.environ["NUMBER_OF_BROKERS"])):
        temp = [x for x in ip_list1 if x!=ip_list1[i]]
        getManager().build_run(ip_list1[i],temp)
        #Manager.lock.acquire()
        # getManager().brokers[broker_obj.brokerID] = broker_obj
        # from api.data_struct import brokerMetaDataLock
        # brokerMetaDataLock[broker_obj.brokerID] = threading.Lock()
        #Manager.lock.release()
        # elif type == 'addOrRestartBroker':
        #     restart = data['restart_']
        #     curr_id = data['curr_id_']
        #     url = data['url_']
        #     docker_id = data['docker_id_']
        #     db_uri = data['db_uri_']
        print("Hoooooooooooooooooooooooooooooooooo")
        
        print("adfsdbdbdndndgnb")



    
scheduler = BackgroundScheduler()
job = scheduler.add_job(getManager().checkBrokerHeartBeat, 'interval', minutes = float(os.environ['HEART_BEAT_INTERVAL']))
job = scheduler.add_job(getManager().checkManagerHeartBeat, 'interval', minutes = 1.1 * float(os.environ['HEART_BEAT_INTERVAL']))
scheduler.start()


from api import routes