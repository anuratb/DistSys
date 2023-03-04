from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import os
import threading
import json
from dotenv import load_dotenv
import psycopg2
load_dotenv()

DB_URI = 'postgresql+psycopg2://anurat:abcd@127.0.0.1:5432/anurat'
DOCKER_DB_URI = 'postgresql+psycopg2://anurat:abcd@127.0.0.1:5432/'
WAL_path = "./temp.txt"


db_username = 'anurat'
if 'DB_USERNAME' in os.environ:
    db_username = os.environ['DB_USERNAME']
db_password = 'abcd'
if 'DB_PASSWORD' in os.environ:
    db_password = os.environ['DB_PASSWORD']
db_host = '127.0.0.1'#TODO get my subnet ip
if 'DB_HOST' in os.environ:
    db_host = os.environ['DB_HOST']
db_port = '5432'
if 'DB_PORT' in os.environ:
    db_port = os.environ['DB_PORT']
docker_img_broker = 'broker_image'
if 'DOCKER_IMG_BROKER' in os.environ:
    docker_img_broker = os.environ['DOCKER_IMG_BROKER']
postgres_container = 'postgres'
if 'POSTGRES_CONTAINER' in os.environ:
    postgres_container = os.environ['POSTGRES_CONTAINER']


Load_from_db = False

def create_app(test_config = None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config = True)
    global DB_URI
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['SECRET_KEY'] = '5791628bb0b13ce0c676dfde280ba245'
    app.config['SQLALCHEMY_DATABASE_URI'] = DB_URI

    if 'LOAD_FROM_DB' in os.environ.keys():
        global Load_from_db
        Load_from_db = os.environ['LOAD_FROM_DB']
    
    if ('DOCKER_DB_URI' in os.environ.keys()):        
        DOCKER_DB_URI = os.environ['DOCKER_DB_URI']
    #print(str(os.system("docker inspect '{}'".format(postgres_container))))
    #print(postgres_container)
    obj = json.loads(os.popen("docker inspect '{}'".format(postgres_container)).read())
    #print(obj[0].keys())
    global db_host
    #print(obj[0]['NetworkSettings']['IPAddress'])
    db_host = obj[0]['NetworkSettings']['IPAddress']
    global db_port
    db_port = 5432
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
    DB_URI = DOCKER_DB_URI + 'anurat'
    db = SQLAlchemy(app)
    return app, db

app, db = create_app()

from api.data_struct  import TopicMetaData,ProducerMetaData,ConsumerMetaData,BrokerMetaData,Docker,Manager
from api.models import TopicDB,TopicBroker,BrokerMetaDataDB,globalProducerDB,globalConsumerDB,localProducerDB,localConsumerDB

def load_from_db():

    ############################ Write Pending commits to DB ######################################
    walFile = open(WAL_path, "r+")
    for line in walFile.readlines():
        exec(line)
    db.commit.all()
    #erase the file contents
    walFile.truncate(0)
    walFile.close()
    ###############################################################################################




    ################################### Load Topic MetaData  ########################################
    topicMetaData = TopicMetaData()
    for topic in TopicDB.query.all():
        topicMetaData.Topics[topic.topicName] = (topic.id,topic.numPartitions)
    for topic in TopicBroker.query.all():
        topicMetaData.PartitionBroker[str(topic.partition)+"#"+topic.topic] = topic.brokerId
    topicMetaData.lock = threading.Lock()
    for broker in BrokerMetaDataDB.query.all():
        topicMetaData.BrokerUrls.insert(broker.url)
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
            agg[local.broker_id].append(local.local_id)
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
            agg[local.broker_id].append(local.local_id)
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
    brokersDocker = Docker()
    Manager.brokers = {}
    for broker in BrokerMetaDataDB.query.all():
        curr = BrokerMetaData(
            broker.db_id,
            broker.url,
            broker.docker_name,
            broker.id,
            broker.docker_id

        )
        Manager.brokers[broker.id] = curr
        brokersDocker.brokers[broker.docker_name] = curr.copy()
    ###########################################################################################



    #print(Queue.queue)
# a simple page that says hello
@app.route('/hello1')
def hello1():
    return "Hello"

app.app_context().push()

if(Load_from_db):load_from_db()
else:
    db.create_all()
    
from api import routes

 

