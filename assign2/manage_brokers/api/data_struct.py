import threading
from . import db
import json
import os, requests, time
from random import random
from api.models import TopicDB,TopicBroker,globalProducerDB,localProducerDB,globalConsumerDB,localConsumerDB,BrokerMetaDataDB,DockerDB
from sqlalchemy_utils.functions import database_exists
from api import db_host,db_port,db_password,db_username,docker_img_broker
import subprocess

import psycopg2


WAL_path = './temp.txt'

file = open('WAL_path.txt','w')





BROKER_URL = "http://127.0.0.1:5124"

class TopicMetaData:

    def __init__(self):
        # Map from topicName to {topicID, numPartitions}
        self.Topics = {}
        # A map from partiton#topicName to the corresponding brokerID
        self.PartitionBroker = {}
        self.lock = threading.Lock()

        #No DB updates as assuming broker already created


    # Adds a topic and randomly decides number of paritions
    def addTopic(self, topicName):
        # TODO get the current number of brokers numBrokers
        numBrokers = len(Manager.brokers)
        numPartitions = int(random() * numBrokers)
        if not numPartitions: numPartitions = 1

        self.lock.acquire()
        if topicName in self.Topics:
            self.lock.release()
            raise Exception(f'Topicname: {topicName} already exists')
        topicID = len(self.Topics) + 1
        self.Topics[topicName] = [topicID, numPartitions]
        self.lock.release()

        ########### DB update ##############
        obj = TopicDB(topicName=topicName,numPartitions=numPartitions,topic_id=topicID)
        file.write("db.session.add(TopicDB(topicName={},numPartitions={},topic_id={}))".format(topicName,numPartitions,topicID))
        db.session.add(obj)
        db.session.commit()
        #########################################


        # Choose the brokers (TODO how?)
        Manager.assignBrokers(self.PartitionBroker,topicName, numPartitions)


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
        # corresponding (prodIDs, partitionNo)
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
        file.write("db.session.add(globalProducerDB(glob_id={},topic = {},rrindex=0,brokerCnt = {}))".format(clientID,topicName,len(clientIDs)))
        for row in clientIDs:
            broker_id = row[0] 
            prod_ids = row[1:]
            local_prods = []
            for prod_id,partition in prod_ids:
                local_prods.append(localProducerDB(local_id = prod_id,broker_id = broker_id,glob_id = clientID,partition=partition))
                file.write("db.session.add(localProducerDB(local_id = {},broker_id = {},glob_id = {},partition={}))".format(prod_id,broker_id,clientID,partition))
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

    '''
    Returns the brokerID and prodID to which the message should be sent
    '''
    def getRRIndex(self, clientID, topicName):
        #TODO

        globProd = globalProducerDB.query.filter_by(glob_id=clientID,topic=topicName).first()
        #current_index = globProd.rrindex
        cnt = len(globProd.localProducer)
        ind = int(random() * (cnt))
        '''
        obj = BrokerMetaDataDB.query.filter_by(broker_id=current_index).first()
        L = len(BrokerMetaDataDB.query.filter_by(broker_id=current_index))
        partitionID = 0
        if L > 2:
            partitionID = int(random() * (L - 1))
        brokerID = obj.broker_id
        '''
        localProd = globProd.localProducer[ind]
        prodID= localProd.local_id
        partition = localProd.partition
        brokerID = localProd.broker_id
        #current_index = (current_index + 1) % cnt
        ########### DB update ##############
        #globalProducerDB.query.filter_by(glob_id=clientID,topic=topicName).first().rrindex=current_index
        #db.session.commit()
        #########################################
        return brokerID, prodID,partition

        if topicName not in Manager.topicMetaData.Topics:
            raise Exception(f"Topic {topicName} doesn't exist")
        K = clientID + "#" + topicName
        self.rrIndexLock[K].acquire()
        nextBroker = self.rrIndex[K]
        self.rrIndex[K] = (self.rrIndex[K] + 1) % len(self.subscription[K])
        self.rrIndexLock[K].release()
        
        index = 0
        if L > 2:
            index = int(random() * (L - 1))
        return self.subscription[K][nextBroker][0], self.subscription[K][nextBroker][index + 1][0], self.subscription[K][nextBroker][index + 1][1]


class ConsumerMetaData:
    def __init__(self, cnt = 0):
        # Only stores those clients that subscribe to enitre topic
        # Map from clienID#TopicName to an array X. X[i] contains an array containing brokerID followed by
        # corresponding (conIDs, partitionNo)
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
        file.write("db.session.add(globalConsumerDB(glob_id={},topic = {},rrindex=0,brokerCnt = {}))".format(clientID,topicName,len(clientIDs)))
        for row in clientIDs:
            broker_id = row[0] 
            prod_ids = row[1:]
            local_prods = []
            for prod_id,partition in prod_ids:
                local_prods.append(localConsumerDB(local_id = prod_id,broker_id = broker_id,glob_id = clientID,partition=partition))
                file.write("db.session.add(localConsumerDB(local_id = {},broker_id = {},glob_id = {},partition={}))".format(prod_id,broker_id,clientID,partition))
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
        index = 0
        if L > 2:
            index = int(random() * (L - 1))
        return self.subscription[K][nextBroker][0], self.subscription[K][nextBroker][index + 1][0], self.subscription[K][nextBroker][index + 1][1]

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
    lock = threading.Lock()
    brokers = {}

    @classmethod
    def assignBrokers(cls, PartitionBroker, topicName, numPartitions):
        if(len(cls.brokers) < numPartitions):
            #create partitions on demand
            print("Creating broker ")
            for i in range(numPartitions - len(cls.brokers)):
                broker_obj = brokersDocker.build_run("../../broker")
                cls.lock.acquire()
                cls.brokers[broker_obj.brokerID] = broker_obj
                cls.lock.release()
        print('###################')
        brokerIDs = list(cls.brokers.keys())
        l = len(brokerIDs)
        brokerList = []
        for i in range(numPartitions):
            brokerList.append(brokerIDs[int(random() * l)])
        print('###################')
        for i in range(numPartitions):
            # TODO assign the broker url
            brokerID = brokerList[i]
            brokerTopicName = str(i + 1) + '#' + topicName
            PartitionBroker[brokerTopicName] = brokerID
            ####################### DB UPDATES ###########################
            db.session.add(TopicBroker(
                topic = topicName,
                brokerID = brokerID,
                partition = i+1))
            file.write("db.session.add(TopicBroker(topic = {},brokerID = {},partition = {}))".format(topicName,brokerID,i+1))    
        db.session.commit()
            
        print('###################')
        
        # POST request to each broker to create new topic
        for i in range(numPartitions):
            brokerTopicName = str(i + 1) + '#' + topicName
            print(brokerTopicName)
            cur_url= cls.brokers[PartitionBroker[brokerTopicName]].url.split(':')[1].split('/')[-1]
            while(True):
                time.sleep(1)
                res = os.system("ping -c 1 "+cur_url)
                if(res == 0):
                    break
            print(cls.brokers[PartitionBroker[brokerTopicName]].url)
            res = requests.post(cls.brokers[PartitionBroker[brokerTopicName]].url + "/topics", 
            json={
                "name": brokerTopicName
            })
            print(res.json())
            # Broker failure (TODO: what to do?)
            if res.status_code != 200:
                print("Hello")
                brokersDocker.restartBroker(cls.brokers[PartitionBroker[brokerTopicName]].brokerID)
                brokerTopicName = str(i + 1) + '#' + topicName
                res = requests.post(cls.brokers[PartitionBroker[brokerTopicName]].url + "/topics", 
                json={
                    "name": brokerTopicName
                })
            elif(res.json().get("status") != "Success"):
                pass

    @classmethod
    def getBrokerUrl(cls, topicName, partition):
        subbedTopicName = cls.topicMetaData.getSubbedTopicName(topicName, partition)
        brokerID = cls.topicMetaData.getBrokerID(subbedTopicName)
        return cls.getBrokerUrlFromID(brokerID)


    @classmethod
    def registerClientForAllPartitions(cls, url, topicName, isProducer):
        if topicName not in Manager.topicMetaData.Topics.keys():
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
                brokerMap[brokerID].append((ID, i))

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
        #To be done by read only manager
        return BrokerMetaDataDB.query.filter_by(broker_id = brokerID).first().url
        #return cls.brokers[brokerID].url

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
        # Maps Docker Name to Broker Object --> Not needed
        #self.id ={}
        self.lock = threading.Lock()

    def build_run(self,path:str):
        self.lock.acquire()
        curr_id = self.cnt
        self.cnt+=1
        self.lock.release()
        broker_nme = "broker"+str(curr_id)
        if(database_exists('postgresql://{}:{}@{}:{}/{}'.format(db_username,db_password,db_host,db_port,broker_nme))):
            conn = psycopg2.connect(
                user=db_username, password=db_password, host=db_host, port= db_port
            )
            conn.autocommit = True

            cursor = conn.cursor()
            sql = '''DROP database {};'''.format(broker_nme)
            cursor.execute(sql)
            
            conn.close()
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
        
            

        db_uri = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(db_username,db_password,db_host,db_port,broker_nme)
        #obj = os.system("docker build -t {}:latest {} --build-arg DB_URI={}".format("broker"+str(curr_id),path,str(db_uri)))

        ###################### DB UPDATES ############################
        #
        #obj  = DockerDB()
        #file.write("db.session.add(DockerDB()")
        #db.session.add(obj)
        #db.session.commit()
        ##############################################################
        docker_id = 0
        print("broker"+str(curr_id),db_uri,docker_img_broker)
        os.system("docker run --name {} -d -p 0:5124 --expose 5124 -e DB_URI={} {}".format("broker"+str(curr_id),db_uri,docker_img_broker))
        print("docker run --name {} -d -p 0:5124 --expose 5124 -e DB_URI={} {}".format("broker"+str(curr_id),db_uri,docker_img_broker))
        obj = subprocess.Popen("docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' broker"+str(curr_id), shell=True, stdout=subprocess.PIPE).stdout.read()
        url = 'http://' + obj.decode('utf-8').strip() + ':5124'
        print(url)

        #self.lock.acquire()
        #self.id[broker_nme] = BrokerMetaData(db_uri,url,"broker"+str(curr_id))
        #self.lock.release()
        #TopicMetaData.lock.aquire()
        #TopicMetaData.addBrokerUrl(url)
        #TopicMetaData.lock.release()
        broker_obj = BrokerMetaData(db_uri,url,"broker"+str(curr_id),curr_id,docker_id)
        ################# DB UPDATES ########################
        
        obj = BrokerMetaDataDB(
            broker_id = broker_obj.brokerID, 
            url = broker_obj.url,
            db_uri = broker_obj.DB_URI,
            docker_name = broker_obj.docker_name,
            last_beat = broker_obj.last_beat,
            docker_id = broker_obj.docker_id,


        )
        file.write("db.session.add(BrokerMetaDataDB(broker_id = {}, url = {},db_uri = {},docker_name = {},last_beat = {},docker_id = {}))".format(broker_obj.brokerID, broker_obj.url,broker_obj.DB_URI,broker_obj.docker_name,broker_obj.last_beat,broker_obj.docker_id))
        db.session.add(obj)
        db.session.commit()
        #####################################################
        return broker_obj

    def restartBroker(self,brokerId):
        self.lock.acquire()
        curr_id = cnt
        cnt+=1
        self.lock.release()
        new_broker_nme = "broker"+str(curr_id)
        oldBrokerObj = Manager.brokers[brokerId]

        ############## Connect to Database #######################
        #conn = psycopg2.connect(
        #    user=db_username, password=db_password, host=db_host, port= db_port
        #)
        #conn.autocommit = True
        #
        #cursor = conn.cursor()
        #sql = '''CREATE database {};'''.format(broker_nme)
        #cursor.execute(sql)
        #
        #conn.close()
        ####################################################

        db_uri = oldBrokerObj.DB_URI
        #obj = os.system("docker build -t {}:latest {} --build-arg DB_URI={}".format("broker"+str(curr_id),path,str(db_uri)))

        ###################### DB UPDATES ############################
        #
        #obj  = DockerDB()
        #file.write("db.session.add(DockerDB()")
        #db.session.add(obj)
        #db.session.commit()
        ##############################################################
        docker_id = 0

        obj = os.system("docker run --name {} -d -p 0:5124 --expose 5124 -e DB_URI={} {}".format(new_broker_nme,db_uri,docker_img_broker))
        url = json.loads(str(obj))["NetworkSettings"]["IPAddress"]+":5124/"
        self.lock.acquire()
        #self.id[new_broker_nme] = BrokerMetaData(db_uri,url,"broker"+str(curr_id))
        #self.lock.release()
        #TopicMetaData.lock.aquire()
        #TopicMetaData.addBrokerUrl(url)
        #TopicMetaData.lock.release()
        Manager[brokerId].docker_name = new_broker_nme
        Manager[brokerId].url = url
        Manager[brokerId].docker_id = docker_id
        ################# DB UPDATES ########################
        BrokerMetaDataDB.query.filter_by(broker_id = brokerId).update(dict(docker_name = new_broker_nme,url = url,docker_id = docker_id))
        file.write("BrokerMetaDataDB.query.filter_by(broker_id = {}).update(dict(docker_name = {},url = {},docker_id = {}))".format(brokerId,new_broker_nme,url,docker_id))
        db.session.commit()
        #####################################################
        return Manager[brokerId]
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

