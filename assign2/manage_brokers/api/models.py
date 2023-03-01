'''
Broker_id| Url |




Topic



Partition 

'''
from api import db


####################### FOR TOPIC METADATA #########################

    
class TopicDB(db.model):
    topic_id = db.Column(db.Integer,primary_key=True,nullable=False)
    topicName = db.Column(db.String,primary_key=False,nullable=False)
    numPartitions = db.Column(db.Integer,nullable=False)
    #topicMetaData_id = db.Column(db.Integer,db.ForeignKey('TopicMetadataDB.id'))
class TopicBroker(db.model):
    id = db.Column(db.Integer,primary_key=True,nullable=False)
    topic = db.Column(db.String,primary_key=False,nullable=False)
    partition = db.Column(db.Integer,primary_key=False,nullable=False)
    brokerID = db.Column(db.Integer,primary_key = False,nullable=False)
    #topic_id = db.Column(db.Integer,db.ForeignKey('TopicMetadataDB.id'))

#class BrokerURL(db.model):
#    brokerURL = db.Column(db.string,primary_key=True,nullable = False)
#    topic_id = db.Column(db.Integer,db.ForeignKey('TopicMetadataDB.id'))


######################### FOR PRODUCER METADATA #################################
'''
global_id
topic
broker_id
local_id
'''



class globalProducerDB(db.model):
    glob_id = db.Column(db.Integer,primary_key=True,nullable=False)
    rrindex = db.Column(db.Integer,nullable=False)
    topic = db.Column(db.String,nullable=False)
    localProducer = db.relationship('localProducerDB',backref='globalProducer',lazy=True)
    brokerCnt = db.Column(db.Integer,nullable=False)

class localProducerDB(db.model):
    id = db.Column(db.Integer,primary_key=True,nullable=False)
    local_id = db.Column(db.Integer,primary_key=False,nullable=False)
    broker_id = db.Column(db.Integer,db.ForeignKey('BrokerMetaDataDB.brokerID'),nullable=False)
    glob_id = db.Column(db.Integer,db.ForeignKey('globalProducerDB.glob_id'),nullable=False)



'''

class ProducerMetaDataDB(db.model):
    id = db.Column(db.Integer,primary_key=True,nullable=False)
    subscription = db.relationship('ProdSubscribe',backref='prodMetaData',lazy=True)
    

class ProdSubscribe(db.model):
    id = db.Column(db.String,primary_key=True,nullable=False) # producer id # topicName
    rrIndex = db.Column(db.Integer,nullable=False)
    brokerList = db.relationship('ProdTopicBroker',backref='ProdSubscribe',lazy = True)
    prodMetadataId = db.Column(db.Integer,db.ForeignKey('ProducerMetadata.id'))

class ProdTopicBroker(db.model):
    brokerUrl = db.Column(db.Integer,primary_key=True,nullable=False)
    prodId = db.Column(db.Integer,nullable=False)
    ProdSubscribeId = db.Column(db.Integer,db.ForeignKey('ProdSubscribe.id'))
'''
######################### FOR CONSUMER METADATA #################################

class globalConsumerDB(db.model):
    glob_id = db.Column(db.Integer,primary_key=True,nullable=False)
    rrindex = db.Column(db.Integer,nullable=False)
    topic = db.Column(db.String,nullable=False)
    localConsumer = db.relationship('localConsumerDB',backref='globalConsumer',lazy=True)
    brokerCnt = db.Column(db.Integer,nullable=False)

class localConsumerDB(db.model):
    id = db.Column(db.Integer,primary_key=True,nullable=False)
    local_id = db.Column(db.Integer,primary_key=False,nullable=False)
    broker_id = db.Column(db.Integer,db.ForeignKey('BrokerMetaDataDB.brokerID'),nullable=False)
    glob_id = db.Column(db.Integer,db.ForeignKey('globalConsumerDB.glob_id'),nullable=False)

'''
class ConsumerMetaDataDB(db.model):
    id = db.Column(db.Integer,primary_key=True,nullable=False)
    subscription = db.relationship('ConSubscribe',backref='conMetaData',lazy=True)
    

class ConSubscribe(db.model):
    id = db.Column(db.Integer,primary_key=True,nullable=False)
    rrIndex = db.Column(db.Integer,nullable=False)
    brokerList = db.relationship('ConTopicBroker',backref='ConSubscribe',lazy = True)
    conMetadataId = db.Column(db.Integer,db.ForeignKey('ConsumerMetadata.id'))

class ConTopicBroker(db.model):
    brokerUrl = db.Column(db.Integer,primary_key=True,nullable=False)
    conId = db.Column(db.Integer,nullable=False)
    ConSubscribeId = db.Column(db.Integer,db.ForeignKey('ConSubscribe.id'))

'''

############################## FOR MANAGER ###############################
#class ManagerDB(db.model):
#    id = db.Column(db.Integer,primary_key=True,nullable=False)


############################### BROKER META DATA ##################

class BrokerMetaDataDB(db.model):
    db_uri = db.Column(db.String,primary_key=False,nullable=False)
    broker_id = db.Column(db.Integer,primary_key=True,nullable=False)
    url = db.Column(db.String,nullable=False)
    docker_name = db.Column(db.String,nullable=False)
    last_beat = db.Column(db.Float,nullable=False)
    docker_id = db.Column(db.Integer,db.ForeignKey('DockerDB.id'))
    localProd = db.relationship('localProducerDB',backref='broker',lazy=True)
    localCons = db.relationship('localConsumerDB',backref='broker',lazy=True)

############################### DOCKER METADATA ##############################################
class DockerDB(db.model):
    id = db.Column(db.Integer,primary_key=True,nullable=False)
    brokers = db.relationship('BrokerMetaDataDB',backref='docker',lazy=True)
    