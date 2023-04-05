'''
Broker_id| Url |




Topic



Partition 

'''
from api import db


####################### FOR TOPIC METADATA #########################

    
class TopicDB(db.Model):
    topic_id = db.Column(db.Integer,primary_key=True,nullable=False)
    topicName = db.Column(db.String,primary_key=False,nullable=False)
    numPartitions = db.Column(db.Integer,nullable=False)
    rrindex = db.Column(db.Integer,nullable=False)
    #topicMetaData_id = db.Column(db.Integer,db.ForeignKey('TopicMetadataDB.id'))
class TopicBroker(db.Model):
    id = db.Column(db.Integer,primary_key=True,nullable=False)
    topic = db.Column(db.String,primary_key=False,nullable=False)
    partition = db.Column(db.Integer,primary_key=False,nullable=False)
    brokerID = db.Column(db.Integer,primary_key = False,nullable=False)
    #topic_id = db.Column(db.Integer,db.ForeignKey('TopicMetadataDB.id'))

#class BrokerURL(db.Model):
#    brokerURL = db.Column(db.string,primary_key=True,nullable = False)
#    topic_id = db.Column(db.Integer,db.ForeignKey('TopicMetadataDB.id'))


######################### FOR PRODUCER METADATA #################################
'''
global_id
topic
broker_id
local_id
'''



class globalProducerDB(db.Model):
    glob_id = db.Column(db.String,primary_key=True,nullable=False)
    rrindex = db.Column(db.Integer,nullable=False)
    topic = db.Column(db.String,nullable=False)
    localProducer = db.relationship('localProducerDB',backref='globalProducer',lazy=True)
    brokerCnt = db.Column(db.Integer,nullable=False)

class localProducerDB(db.Model):
    id = db.Column(db.Integer,primary_key=True,nullable=False)
    local_id = db.Column(db.Integer,primary_key=False,nullable=False)
    broker_id = db.Column(db.Integer,db.ForeignKey('broker_meta_data_db.broker_id'),nullable=False)
    glob_id = db.Column(db.String,db.ForeignKey('global_producer_db.glob_id'),nullable=False)
    partition = db.Column(db.Integer,nullable=False)



'''

class ProducerMetaDataDB(db.Model):
    id = db.Column(db.Integer,primary_key=True,nullable=False)
    subscription = db.relationship('ProdSubscribe',backref='prodMetaData',lazy=True)
    

class ProdSubscribe(db.Model):
    id = db.Column(db.String,primary_key=True,nullable=False) # producer id # topicName
    rrIndex = db.Column(db.Integer,nullable=False)
    brokerList = db.relationship('ProdTopicBroker',backref='ProdSubscribe',lazy = True)
    prodMetadataId = db.Column(db.Integer,db.ForeignKey('ProducerMetadata.id'))

class ProdTopicBroker(db.Model):
    brokerUrl = db.Column(db.Integer,primary_key=True,nullable=False)
    prodId = db.Column(db.Integer,nullable=False)
    ProdSubscribeId = db.Column(db.Integer,db.ForeignKey('ProdSubscribe.id'))
'''
######################### FOR CONSUMER METADATA #################################

class globalConsumerDB(db.Model):
    glob_id = db.Column(db.String,primary_key=True,nullable=False)
    rrindex = db.Column(db.Integer,nullable=False)
    topic = db.Column(db.String,nullable=False)
    localConsumer = db.relationship('localConsumerDB',backref='globalConsumer',lazy=True)
    brokerCnt = db.Column(db.Integer,nullable=False)

class localConsumerDB(db.Model):
    id = db.Column(db.Integer,primary_key=True,nullable=False)
    local_id = db.Column(db.Integer,primary_key=False,nullable=False)
    broker_id = db.Column(db.Integer,db.ForeignKey('broker_meta_data_db.broker_id'),nullable=False)
    glob_id = db.Column(db.String,db.ForeignKey('global_consumer_db.glob_id'),nullable=False)
    partition = db.Column(db.Integer,nullable=False)

'''
class ConsumerMetaDataDB(db.Model):
    id = db.Column(db.Integer,primary_key=True,nullable=False)
    subscription = db.relationship('ConSubscribe',backref='conMetaData',lazy=True)
    

class ConSubscribe(db.Model):
    id = db.Column(db.Integer,primary_key=True,nullable=False)
    rrIndex = db.Column(db.Integer,nullable=False)
    brokerList = db.relationship('ConTopicBroker',backref='ConSubscribe',lazy = True)
    conMetadataId = db.Column(db.Integer,db.ForeignKey('ConsumerMetadata.id'))

class ConTopicBroker(db.Model):
    brokerUrl = db.Column(db.Integer,primary_key=True,nullable=False)
    conId = db.Column(db.Integer,nullable=False)
    ConSubscribeId = db.Column(db.Integer,db.ForeignKey('ConSubscribe.id'))

'''

############################## FOR MANAGER ###############################
#class ManagerDB(db.Model):
#    id = db.Column(db.Integer,primary_key=True,nullable=False)


############################### BROKER META DATA ##################

class BrokerMetaDataDB(db.Model):
    db_uri = db.Column(db.String,primary_key=False,nullable=False)
    broker_id = db.Column(db.Integer,primary_key=True,nullable=False)
    url = db.Column(db.String,nullable=False)
    docker_name = db.Column(db.String,nullable=False)
    last_beat = db.Column(db.Float,nullable=False)
    docker_id = db.Column(db.Integer,db.ForeignKey('docker_db.id'))
    localProd = db.relationship('localProducerDB',backref='broker',lazy=True)
    localCons = db.relationship('localConsumerDB',backref='broker',lazy=True)

############################### DOCKER METADATA ##############################################
class DockerDB(db.Model):
    id = db.Column(db.Integer,primary_key=True,nullable=False)
    brokers = db.relationship('BrokerMetaDataDB',backref='docker',lazy=True)

############################### FOR Manager ##############################################
class ManagerDB(db.Model):
    id = db.Column(db.String,primary_key=True,nullable=False)
    url = db.Column(db.String,nullable=False)
    

############################### Replication ##############################################

replication_table = db.Table('user_group',
                      db.Column('master_id', db.Integer, db.ForeignKey('ReplicationDB.id')),
                      db.Column('slave_id', db.Integer, db.ForeignKey('ReplicationDB.id')))


class ReplicationDB(db.Model):
    id = db.Column(db.Integer,primary_key=True,nullable=False)
    topic = db.Column(db.String,nullable=False)
    partition = db.Column(db.Integer,nullable=False)
    replicas = db.relationship(
        'ReplicationDB',
        secondary=replication_table,
        primaryjoin=(replication_table.c.master_id == id),
        secondaryjoin=(replication_table.c.slave_id == id),
        backref=db.backref('replication',lazy='dynamic'),
        lazy='dynamic')
    