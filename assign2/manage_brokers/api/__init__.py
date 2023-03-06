from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import os
import threading
import json
import dotenv
from dotenv import load_dotenv
import psycopg2
import random
from sqlalchemy_utils.functions import database_exists
from api.utils import *

dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)

if 'IS_WRITE' not in os.environ.keys():
    os.environ['IS_WRITE'] = '1'

DB_URI = 'postgresql+psycopg2://anurat:abcd@127.0.0.1:5432/anurat'
DOCKER_DB_URI = 'postgresql+psycopg2://anurat:abcd@127.0.0.1:5432/'
WAL_path = "./temp.txt"



db_username = os.environ['DB_USERNAME']

db_password = os.environ['DB_PASSWORD']

db_host = os.environ['DB_HOST']

db_port = os.environ['DB_PORT']


docker_img_broker = os.environ['DOCKER_IMG_BROKER']

postgres_container = os.environ['POSTGRES_CONTAINER']
print(docker_img_broker,db_username,db_host)

Load_from_db = os.environ["LOAD_FROM_DB"]=='True'
print(Load_from_db)

def create_app(test_config = None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config = True)
    global DB_URI
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['SECRET_KEY'] = '5791628bb0b13ce0c676dfde280ba245'
    app.config['SQLALCHEMY_DATABASE_URI'] = DB_URI

    
    
    if ('DOCKER_DB_URI' in os.environ.keys()):        
        DOCKER_DB_URI = os.environ['DOCKER_DB_URI']
    #print(str(os.system("docker inspect '{}'".format(postgres_container))))
    #print(postgres_container)
    obj = json.loads(os.popen("docker inspect '{}'".format(postgres_container)).read())
    #print(obj[0].keys())
    #global db_host
    #print(obj[0]['NetworkSettings']['IPAddress'])
    #db_host = obj[0]['NetworkSettings']['IPAddress']
    #global db_port
    #db_port = 5432
    DOCKER_DB_URI = 'postgresql+psycopg2://'+db_username+':'+db_password+'@'+db_host+':'+str(db_port)+'/'
    '''
    if(Load_from_db == False):
        ############## Create Database #######################
        conn = psycopg2.connect(
            user=db_username, password=db_password, host=db_host, port= db_port
        )
        conn.autocommit = True

        cursor = conn.cursor()
        sql = '''
        #CREATE database {}
    '''     .format('anurat')
        cursor.execute(sql)
        
        conn.close()
        DB_URI = DOCKER_DB_URI + 'anurat'
        ####################################################
    '''
    #db = None
    DB_URI = DOCKER_DB_URI + os.environ['DB_NAME']
    app.config['SQLALCHEMY_DATABASE_URI'] = DB_URI
    db = SQLAlchemy(app)
    return app, db

app, db = create_app()

from api.data_struct  import TopicMetaData,ProducerMetaData,ConsumerMetaData,BrokerMetaData,Docker,Manager
from api.models import ManagerDB,TopicDB,TopicBroker,BrokerMetaDataDB,globalProducerDB,globalConsumerDB,DockerDB,localProducerDB,localConsumerDB
###############################GLOBALS#####################################

#brokersDocker = Docker()
IsWriteManager = (int(os.environ["IS_WRITE"])==1)
random.seed(int(os.environ['RANDOM_SEED']))
globLock = threading.Lock()
# cntManager = len(ManagerDB.query.all())
readManagerURL = []

###########################################################################
def create_read_manager():
    
    manager_nme = "manager"+str(cntManager)
    #db_uri = create_postgres_db(manager_nme,manager_nme+"_db",db_username,db_password)    
    db_uri = DOCKER_DB_URI
    url = create_container(manager_nme,db_uri,docker_img_broker,envs={
        'IS_WRITE':'0'
    })      
    print(url)
    
    ################# DB UPDATES ########################
    
    db.session.add(ManagerDB(manager_nme,url))
    db.session.commit()
    #####################################################

    globLock.acquire()
    cntManager+=1
    readManagerURL.append(url)
    globLock.release()
    return url

    
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
    cntManager = len(ManagerDB.query.all())
    for obj in ManagerDB.query.all():
        readManagerURL.append(obj.url)



app.app_context().push()

if(Load_from_db):
    load_from_db()
else:
    db.drop_all()
    db.create_all()
    db.session.add(DockerDB(id=0))
    db.session.commit()


if os.environ['EXECUTE'] == '0':
    os.environ['EXECUTE'] = '1'
    # if IsWriteManager:
    #     for _ in range(int(os.environ["NUMBER_READ_MANAGERS"])):
    #         create_read_manager()

    # for _ in range(int(os.environ["NUMBER_OF_BROKERS"])):

    #     broker_obj = Docker.build_run("../../broker")
    #     Manager.lock.acquire()
    #     Manager.brokers[broker_obj.brokerID] = broker_obj
    #     Manager.lock.release()

   
from api import routes

 

