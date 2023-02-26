import threading
from . import db
import os, requests
from random import random


db_username = 'anurat'
db_password = 'abcd'
db_host = '127.0.0.1'
db_port = '5432'

import psycopg2

BROKER_URL = "http://127.0.0.1:5124"

class TopicMetaData:

    def __init__(self):
        # Map from topicName to {topicID, numPartitions}
        self.Topics = {}
        # A map from partiton#topicName to the corresponding broker url
        self.PartitionBroker = {}
        self.lock = threading.Lock()

    # Adds a topic and randomly decides number of paritions
    def addTopic(self, topicName):
        # TODO get the current number of brokers numBrokers
        numBrokers = 5
        numPartitions = int(random() * numBrokers)
        if not numPartitions: numPartitions = 1

        self.lock.acquire()
        if topicName in self.Topics:
            self.lock.release()
            raise Exception(f'Topicname: {topicName} already exists')
               
        self.Topics[topicName] = [len(self.Topics) + 1, numPartitions]
        self.lock.release()

        # Choose the brokers (TODO how?)
        for i in range(1, numPartitions + 1):
            # TODO assign the broker url
            brokerTopicName = str(i) + '#' + topicName
            self.PartitionBroker[brokerTopicName] = BROKER_URL
        
        # POST request to each broker to create new topic
        for i in range(self.Topics[topicName][1]):
            brokerTopicName = str(i + 1) + '#' + topicName
            res = requests.post(self.PartitionBroker[brokerTopicName] + "/topics", json={
                "name": brokerTopicName
            })

            # Broker failure (TODO: what to do?)
            if res.status_code != 200:
                pass
            elif(res.json().get("status") != "Success"):
                pass


    def getSubbedTopicName(self, topicName, partition = 0):
        if topicName not in self.Topics:
            raise Exception(f"Topicname: {topicName} doesn't exist")
        ret = str(partition) + '#' + topicName
        if partition:
            if ret not in self.PartitionBroker:
                raise Exception(f"Invalid partition for topic: {topicName}")
        return ret

    def getBrokerUrl(self, subbedTopicName):
        return self.PartitionBroker[subbedTopicName]

    def getTopicsList(self):
        return self.Topics

class ProducerMetaData:
    
    def __init__(self, producerCnt_ = 0):
        # Only stores those producers that subscribe to enitre topic
        # Map from producerID#TopicName to producerIDs in different brokers
        self.subscription = {}
        self.producerCnt = producerCnt_
        self.lock = threading.Lock()

    def addSubscription(self, producerIDs, topicName):
        self.lock.acquire()
        self.subscription["$" + str(self.producerCnt) + "#" + topicName] = producerIDs
        self.producerCnt += 1
        self.lock.release()

    def checkSubscription(self, producerID, topicName):
        K = "$" + str(producerID) + "#" + topicName
        if K not in self.subscription:
            raise Exception("Invalid Subscription")
        return self.subscription[K]

class ConsumerMetaData:

    def __init__(self, consumersCnt_ = 0):
        # Only stores those consumers that subscribe to enitre topic
        # Map from consumerID#TopicName to consumerIDs in different brokers
        self.subscription = {}
        self.consumersCnt = consumersCnt_
        self.lock = threading.Lock()

    def addSubscription(self, consumerIDs, topicName):
        self.lock.acquire()
        conID = "$" + str(self.consumersCnt)
        self.subscription[conID + "#" + topicName] = consumerIDs
        self.consumersCnt += 1
        self.lock.release()
        return conID

    def checkSubscription(self, consumerID, topicName):
        K = "$" + str(consumerID) + "#" + topicName
        if K not in self.subscription:
            raise Exception("Invalid Subscription")
        return self.subscription[K]

class Manager:
    topicMetaData = TopicMetaData()
    consumerMetaData = ConsumerMetaData()
    producerMetaData = ProducerMetaData()

    @classmethod
    def getBroker(cls, topicName, partition):
        subbedTopicName = cls.topicMetaData.getSubbedTopicName(topicName, partition)
        return cls.topicMetaData.getBrokerUrl(subbedTopicName)


    @classmethod
    def registerClientForAllPartitions(cls, url, topicName, isProducer):
        numPartitions = Manager.topicMetaData.Topics[topicName][1]
        IDs = []
        for i in range(1, numPartitions + 1):
            brokerTopicName = str(i) + '#' + topicName
            res = requests.post(
                cls.topicMetaData.PartitionBroker[brokerTopicName] + url,
                json={
                    "topic": topicName,
                    "partition": str(i)
                })

            # TODO Broker Failure
            if res.status_code != 200:
                pass
            if(res.json().get("status") != "Success"):
                raise Exception(res.json().get("message"))
            else:
                ID = None
                if isProducer:
                    ID = res.json().get("producer_id")
                else:
                    ID = res.json().get("consumer_id")
                IDs.append(ID)

            if isProducer:
                return cls.producerMetaData.addSubscription(IDs, topicName)
            else:
                return cls.consumerMetaData.addSubscription(IDs, topicName)
            


    @classmethod
    def enqueue(cls, topicName, prodID, msg):
        pass


    @classmethod
    def dequeue(cls, topicName, conID):
        pass

    @classmethod
    def getSize(cls, topicName, conID):
        pass


class Broker:
    def __init__(self, DB_URI = '', url = '', name = ''):
        self.DB_URI = DB_URI
        self.url = url
        self.docker_name = name

class Docker:
    def __init__(self):
        self.cnt = 0
        # Maps Docker Name to Broker Object
        self.id ={}

    def build_run(self,path:str):
        curr_id = cnt
        cnt+=1
        broker_nme = "broker"+str(cnt)

        ##############Create Database#######################
        conn = psycopg2.connect(
            user=db_username, password=db_password, host=db_host, port= db_port
        )
        conn.autocommit = True

        cursor = conn.cursor()
        sql = '''CREATE database {};'''.format(broker_nme)
        cursor.execute(sql)
        
        conn.close()
        ####################################################

        db_uri = 'postgresql+psycopg2:/{}:{}@{}:{}/{}'.format(db_username,db_password,db_host,db_host,broker_nme)
        obj = os.system("docker build -t {}:latest {} --build-arg DB_URI={}".format("broker"+str(curr_id),path,str(db_uri)))
        obj = os.system("docker run {} -p 5000:5005".format("broker"+str(curr_id)))
        url = None
        self.id[cnt] = Broker(db_uri,url,"broker"+str(curr_id))


'''
class VM:
    def __init__(self):
        self.ids = []
    #to return all vm ids
    def get(self):
        return self.ids
'''

###############################GLOBALS#####################################

brokers = Docker()