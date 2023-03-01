import threading
from . import db
import os, requests, time
from random import random
from api import TopicMetadataDB,TopicDB,TopicBroker,BrokerURL,globalProducerDB,localProducerDB,globalConsumerDB,localConsumerDB,BrokerMetaDataDB,DockerDB

db_username = 'anurat'
db_password = 'abcd'
db_host = '127.0.0.1'#TODO get my subnet ip
db_port = '5432'

import psycopg2

BROKER_URL = "http://127.0.0.1:5124"

class TopicMetaData:

    def __init__(self):
        # Map from topicName to {topicID, numPartitions}
        self.Topics = {}
        # A map from partiton#topicName to the corresponding brokerID
        self.PartitionBroker = {}
        self.lock = threading.Lock()
        self.BrokerUrls = set()
    def addBrokerUrl(self,url):
        self.BrokerUrls.insert(url)

        #No DB updates as assuming broker already created


    # Adds a topic and randomly decides number of paritions
    def addTopic(self, topicName):
        # TODO get the current number of brokers numBrokers
        numBrokers = len(self.BrokerUrls)
        numPartitions = int(random() * numBrokers)
        if not numPartitions: numPartitions = 1

        self.lock.acquire()
        if topicName in self.Topics:
            self.lock.release()
            raise Exception(f'Topicname: {topicName} already exists')
               
        self.Topics[topicName] = [len(self.Topics) + 1, numPartitions]
        self.lock.release()
        ########### DB update ##############
        obj = TopicDB(topicName=topicName,numPartitions=numPartitions,topic_id=len(self.Topics)+1,topicMetaData_id=0)
        db.session.add(obj)
        db.session.commit()
        #########################################


        # Choose the brokers (TODO how?)
        Manager.assignBrokers(self.PartitionBroker, numPartitions)


    def getSubbedTopicName(self, topicName, partition = 0):
        if topicName not in self.Topics:
            raise Exception(f"Topicname: {topicName} doesn't exist")
        ret = str(partition) + '#' + topicName
        if partition:
            if ret not in self.PartitionBroker:
                raise Exception(f"Invalid partition for topic: {topicName}")
        return ret

    def getBrokerID(self, subbedTopicName):
        return self.PartitionBroker[subbedTopicName]

    def getTopicsList(self):
        return self.Topics

class ProducerMetaData:
    def __init__(self, cnt = 0):
        # Only stores those clients that subscribe to enitre topic
        # Map from clienID#TopicName to an array X. X[i] contains an array containing brokerID followed by
        # corresponding prodIDs
        self.subscription = {}
        # Map from clientID#TopicName to round-robin index
        self.rrIndex = {}
        self.clientCnt = cnt
        self.subscriptionLock = threading.Lock()
        self.rrIndexLock = {}

    def addSubscription(self, clientIDs, topicName):
        self.subscriptionLock.acquire()
        clientID = "$" + str(self.clientCnt)
        self.clientCnt += 1
        self.subscriptionLock.release()

        K = clientID + "#" + topicName
        self.rrIndex[K] = 0
        self.rrIndexLock[K] = threading.Lock()
        self.subscription[K] = clientIDs
        ################## DB Updates #####################
        glob_prod = globalProducerDB(glob_id=clientID,topic = topicName,rrindex=0,brokerCnt = len(clientIDs))
        for row in clientIDs:
            broker_id = row[0] 
            prod_ids = row[1:]
            local_prods = []
            for prod_id in prod_ids:
                local_prods.append(localProducerDB(local_id = prod_id,broker_id = broker_id,glob_id = clientID))
        db.session.add(glob_prod)
        for local_prod in local_prods:
            db.session.add(local_prod)
        db.session.commit()
        ###################################################


        return clientID

    def checkSubscription(self, clientID, topicName):
        K = clientID + "#" + topicName
        if K not in self.subscription:
            return False
        return True

    def getRRIndex(self, clientID, topicName):
        if topicName not in Manager.topicMetaData.Topics:
            raise Exception(f"Topic {topicName} doesn't exist")
        K = clientID + "#" + topicName
        self.rrIndexLock[K].acquire()
        nextBroker = self.rrIndex[K]
        self.rrIndex[K] = (self.rrIndex[K] + 1) % len(self.subscription[K])
        self.rrIndexLock[K].release()
        
        L = len(self.subscription[K][nextBroker])
        partitionID = 0
        if L > 2:
            partitionID = int(random() * (L - 1))
        return self.subscription[K][nextBroker][0], self.subscription[K][nextBroker][partitionID + 1]
class ConsumerMetaData:
    def __init__(self, cnt = 0):
        # Only stores those clients that subscribe to enitre topic
        # Map from clienID#TopicName to an array X. X[i] contains an array containing brokerID followed by
        # corresponding prodIDs
        self.subscription = {}
        # Map from clientID#TopicName to round-robin index
        self.rrIndex = {}
        self.clientCnt = cnt
        self.subscriptionLock = threading.Lock()
        self.rrIndexLock = {}

    def addSubscription(self, clientIDs, topicName):
        self.subscriptionLock.acquire()
        clientID = "$" + str(self.clientCnt)
        self.clientCnt += 1
        self.subscriptionLock.release()

        K = clientID + "#" + topicName
        self.rrIndex[K] = 0
        self.rrIndexLock[K] = threading.Lock()
        self.subscription[K] = clientIDs

        ################## DB Updates #####################
        glob_prod = globalConsumerDB(glob_id=clientID,topic = topicName,rrindex=0,brokerCnt = len(clientIDs))
        for row in clientIDs:
            broker_id = row[0] 
            prod_ids = row[1:]
            local_prods = []
            for prod_id in prod_ids:
                local_prods.append(localConsumerDB(local_id = prod_id,broker_id = broker_id,glob_id = clientID))
        db.session.add(glob_prod)
        for local_prod in local_prods:
            db.session.add(local_prod)
        db.session.commit()
        ###################################################        


        return clientID

    def checkSubscription(self, clientID, topicName):
        K = clientID + "#" + topicName
        if K not in self.subscription:
            return False
        return True

    def getRRIndex(self, clientID, topicName):#For now no need to update db here
        if topicName not in Manager.topicMetaData.Topics:
            raise Exception(f"Topic {topicName} doesn't exist")
        K = clientID + "#" + topicName
        self.rrIndexLock[K].acquire()
        nextBroker = self.rrIndex[K]
        self.rrIndex[K] = (self.rrIndex[K] + 1) % len(self.subscription[K])
        self.rrIndexLock[K].release()
        
        L = len(self.subscription[K][nextBroker])
        partitionID = 0
        if L > 2:
            partitionID = int(random() * (L - 1))
        return self.subscription[K][nextBroker][0], self.subscription[K][nextBroker][partitionID + 1]

'''
class ProducerMetaData(ClientMetaData):
    
    def __init__(self, producerCnt_ = 0):
        ClientMetaData.__init__(self, producerCnt_)

class ConsumerMetaData(ClientMetaData):

    def __init__(self, consumersCnt_ = 0):
        ClientMetaData.__init__(self, consumersCnt_)
'''
class Manager:
    topicMetaData = TopicMetaData()
   
    consumerMetaData = ConsumerMetaData()
    producerMetaData = ProducerMetaData()
    # A map from broker ID to broker Metadata
    # TODO how to add broker Metadata?
    brokers = {}

    @classmethod
    def assignBrokers(cls, PartitionBroker, topicName, numPartitions):
        #brokerIDs = list(brokers.keys())
        #l = len(brokerIDs)
        #brokerList = []
        for _ in range(numPartitions):
          #  brokerList.append(brokerIDs[int(random() * l)])

        #for brokerID in brokerList:
            # TODO assign the broker url
            broker_obj = None
            try:
                broker_obj = brokersDocker.build_run("../../broker/")
                ################# DB UPDATES ########################
                obj = BrokerMetaDataDB(
                    broker_id = broker_obj.brokerID, 
                    url = broker_obj.brokerURL,
                    db_uri = broker_obj.db_uri,
                    docker_name = broker_obj.docker_name,
                    last_beat = broker_obj.last_beat,
                    docker_id = broker_obj.docker_id,


                )
                db.session.add(obj)
                db.session.commit()
                #####################################################
                brokerID = broker_obj.brokerID

            except:
                pass #TODO
            brokerTopicName = str(i) + '#' + topicName

            PartitionBroker[brokerTopicName] = brokerID
            cls.brokers[brokerID] = broker_obj
        
        # POST request to each broker to create new topic
        for i in range(numPartitions):
            brokerTopicName = str(i + 1) + '#' + topicName
            res = requests.post(PartitionBroker[brokerTopicName] + "/topics", 
            json={
                "name": brokerTopicName
            })

            # Broker failure (TODO: what to do?)
            if res.status_code != 200:
                pass
            elif(res.json().get("status") != "Success"):
                pass

    @classmethod
    def getBrokerUrl(cls, topicName, partition):
        subbedTopicName = cls.topicMetaData.getSubbedTopicName(topicName, partition)
        brokerID = cls.topicMetaData.getBrokerID(subbedTopicName)
        return cls.getBrokerUrlFromID(brokerID)


    @classmethod
    def registerClientForAllPartitions(cls, url, topicName, isProducer):
        if topicName not in Manager.topicMetaData.Topics[topicName]:
            raise Exception(f"Topic {topicName} doesn't exist")

        numPartitions = Manager.topicMetaData.Topics[topicName][1]
        IDs = []
        brokerMap = {}
        for i in range(1, numPartitions + 1):
            brokerTopicName = str(i) + '#' + topicName
            brokerID = cls.topicMetaData.getBrokerID(brokerTopicName)
            brokerUrl = cls.getBrokerUrlFromID(brokerID)
            res = requests.post(
                brokerUrl + url,
                json={
                    "topic": topicName,
                    "partition": str(i)
                })

            # TODO Broker Failure
            if res.status_code != 200:
                pass
            elif(res.json().get("status") != "Success"):
                raise Exception(res.json().get("message"))
            else:
                ID = None
                if isProducer:
                    ID = res.json().get("producer_id")
                else:
                    ID = res.json().get("consumer_id")
                if brokerID not in brokerMap:
                    brokerMap[brokerID] = []
                brokerMap[brokerID].append(ID)

        for brokerID in brokerMap.keys():
            arr = [brokerID]
            arr.extend(brokerMap[brokerID])
            IDs.append(arr)

        if isProducer:
            return cls.producerMetaData.addSubscription(IDs, topicName)
        else:
            return cls.consumerMetaData.addSubscription(IDs, topicName)

    @classmethod      
    def getBrokerUrlFromID(cls, brokerID):
        return cls.brokers[brokerID].url

    @classmethod
    def enqueue(cls, topicName, prodID, msg):
        pass


    @classmethod
    def dequeue(cls, topicName, conID):
        pass

    @classmethod
    def getSize(cls, topicName, conID):
        pass


class BrokerMetaData:
    def __init__(self, DB_URI = '', url = '', name = '',brokerID = 0,docker_id = 0):
        self.DB_URI = DB_URI
        self.url = url
        self.docker_name = name
        self.last_beat = time.monotonic()
        self.brokerID = brokerID
        self.docker_id = docker_id

class Docker:
    def __init__(self):
        self.cnt = 0
        # Maps Docker Name to Broker Object
        self.id ={}
        self.lock = threading.Lock()

    def build_run(self,path:str):
        self.lock.acquire()
        curr_id = cnt
        cnt+=1
        self.lock.release()
        broker_nme = "broker"+str(curr_id)

        ############## Create Database #######################
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

        ###################### DB UPDATES ############################

        obj  = DockerDB()
        db.session.add(obj)
        db.session.commit()
        ##############################################################
        docker_id = obj.id

        obj = os.system("docker run {} -p 5000:5005".format("broker"+str(curr_id)))
        url = None#TODO
        self.lock.acquire()
        self.id[curr_id] = BrokerMetaData(db_uri,url,"broker"+str(curr_id))
        self.lock.release()
        TopicMetaData.lock.aquire()
        TopicMetaData.addBrokerUrl(url)
        TopicMetaData.lock.release()
        return BrokerMetaData(db_uri,url,"broker"+str(curr_id),curr_id,docker_id)

    def removeBroker(self,brokerUrl):
        pass#TODO


'''
class VM:
    def __init__(self):
        self.ids = []
    #to return all vm ids
    def get(self):
        return self.ids
'''

###############################GLOBALS#####################################

brokersDocker = Docker()