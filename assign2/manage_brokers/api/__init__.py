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

from api.data_struct import *
from api.models import *

def load_from_db():
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
    pass
    # db.create_all()
    
from api import routes

 

