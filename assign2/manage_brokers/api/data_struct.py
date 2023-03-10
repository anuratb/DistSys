import threading
from . import db,app
import json
import os, requests, time
from api import random
from api.models import TopicDB,TopicBroker,globalProducerDB,localProducerDB,globalConsumerDB,localConsumerDB,BrokerMetaDataDB,DockerDB
from sqlalchemy_utils.functions import database_exists
from api import db_host,db_port,db_password,db_username,docker_img_broker
import subprocess
from api.utils import is_server_running
import psycopg2

from api.utils import *




LOG_path = './LOG.txt'

file = open('LOG_path.txt','w')






BROKER_URL = "http://127.0.0.1:5124"

class TopicMetaData:

    def __init__(self):
        # Map from topicName to {topicID, numPartitions, rrindex, lock}
        self.Topics = {}
        # A map from partiton#topicName to the corresponding brokerID
        self.PartitionBroker = {}
        self.lock = threading.Lock()

        #No DB updates as assuming broker already created


    # Adds a topic and randomly decides number of paritions
    def addTopic(self, topicName):
        # TODO get the current number of brokers numBrokers
        numBrokers = len(Manager.brokers)
        val = random() * numBrokers
        numPartitions = int(val)
        if  numPartitions == 0: numPartitions = 1

        file.write("INFO Choosing {} partitions for topic {}".format(numPartitions, topicName))

        # Choose the brokers (TODO how?)
        numPartitions, assignedBrokers = Manager.assignBrokers(topicName, numPartitions)

        self.lock.acquire()
        if topicName in self.Topics:
            self.lock.release()
            raise Exception(f'Topicname: {topicName} already exists')
        topicID = len(self.Topics) + 1
        self.Topics[topicName] = [topicID, numPartitions, 0, threading.Lock()]
        self.lock.release()

        # All Brokers are down
        if numPartitions == 0:
            del self.Topics[topicName]
            raise Exception("Service currently unavailable. Please try again later.")
        
        # Now update with actual no of paritions created
        self.Topics[topicName][1] = numPartitions

        partitionNo = 1
        for K in assignedBrokers.keys():
            self.PartitionBroker[K] = assignedBrokers[K]

            ####################### DB UPDATES ###########################
            db.session.add(
                TopicBroker(
                    topic = topicName,
                    brokerID = assignedBrokers[K],
                    partition = partitionNo
                )
            )
            file.write("db.session.add(TopicBroker(topic = {},brokerID = {},partition = {}))".format(topicName, assignedBrokers[K], partitionNo))
            partitionNo += 1
        
        db.session.commit()

        ########### DB update ##############
        obj = TopicDB(topicName=topicName,numPartitions=numPartitions,topic_id=topicID,rrindex=0)
        file.write("db.session.add(TopicDB(topicName={},numPartitions={},topic_id={}, rrindex=0))".format(topicName,numPartitions,topicID))
        db.session.add(obj)
        db.session.commit()
        #########################################



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
        local_prods = []
        for row in clientIDs:
            broker_id = row[0] 
            prod_ids = row[1:]
            
            for prod_id,partition in prod_ids:
                local_prods.append(localProducerDB(local_id = prod_id,broker_id = broker_id,glob_id = clientID,partition=partition))
                file.write("db.session.add(localProducerDB(local_id = {},broker_id = {},glob_id = {},partition={}))".format(prod_id,broker_id,clientID,partition))
    
        cur_rrindex = random()*len(local_prods)
        glob_prod.rrindex = cur_rrindex
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
    def getRRIndex(self, clientID, topicName, rrIndex):
        globProd = globalProducerDB.query.filter_by(glob_id=clientID, topic=topicName).first()

        #To ensure load balancing
        cnt = len(globProd.localProducer)
        ind = (rrIndex + int(globProd.rrindex)) % cnt

        localProd = globProd.localProducer[ind]
        prodID= localProd.local_id
        partition = localProd.partition
        brokerID = localProd.broker_id

        return brokerID, prodID, partition


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
        local_prods = []
        for row in clientIDs:
            broker_id = row[0] 
            prod_ids = row[1:]
            
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

    def getRRIndex(self, clientID, topicName, topic_rrindex):#For now no need to update db here
        if topicName not in [obj.topicName for obj in TopicDB.query.all()]:
            raise Exception(f"Topic {topicName} doesn't exist")
        cons_rrindex = globalConsumerDB.query.filter_by(glob_id=clientID, topic=topicName).first().rrindex
        globCons = globalConsumerDB.query.filter_by(glob_id=clientID,topic=topicName).first()

        #To ensure load balancing
        cnt = len(globCons.localConsumer)
        ind = (cons_rrindex + topic_rrindex) % (cnt)

        localCons = globCons.localConsumer[ind]
        ConsID= localCons.local_id
        partition = localCons.partition
        brokerID = localCons.broker_id

        return brokerID, ConsID,partition


class Manager:
    topicMetaData = TopicMetaData()
    consumerMetaData = ConsumerMetaData()
    producerMetaData = ProducerMetaData()
    lock = threading.Lock()
    # A map from broker ID to broker Metadata
    brokers = {}
    X = 0

    @classmethod
    def assignBrokers(cls, topicName, numPartitions):
        PartitionBroker = {}
        brokerIDs = list(cls.brokers.keys())
        l = len(brokerIDs)
        brokerList = []
        for i in range(numPartitions):
            brokerList.append(brokerIDs[int(random() * l)])

        actualPartitions = 0

        for i in range(numPartitions):
            # TODO assign the broker url
            brokerID = brokerList[i]
            brokerTopicName = str(actualPartitions + 1) + '#' + topicName

            url = cls.brokers[brokerID].url
            res = requests.post(str(url) + "/topics", 
                json = {
                    "name": brokerTopicName
                }
            )

            # Post request to the broker to create the topic

            # Broker failure (TODO: what to do?)
            if res.status_code != 200:
                # Restart the broker
                # executor.submit(Docker.restartBroker, brokerID = brokerID)
                pass
            elif(res.json().get("status") == "Success"):
                PartitionBroker[brokerTopicName] = brokerID
                actualPartitions += 1    
        return actualPartitions, PartitionBroker

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
                # Restart the broker
                # executor.submit(Docker.restartBroker, brokerID = brokerID)
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

        if len(brokerMap) == 0:
            raise Exception("Service currently unavailable. Please try again later.")    
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
    def checkBrokerHeartBeat(cls):
        # requests.get("http://127.0.0.1:5123/crash_recovery", params = {'brokerID': str(cls.X)})
        # cls.X += 1
        # return
        for key,broker in cls.brokers.items():
            val = is_server_running(broker.url)
            if val:
                broker.lock.acquire()
                broker.last_beat = time.monotonic()
                broker.lock.release()
            else:
                # Docker.restartBroker(key)
                _ = requests.get("http://127.0.0.1:5124/crash_recovery", params = {'brokerID': str(broker.brokerID)})
    
    # @classmethod
    # def crashRecovery(cls, brokerID):
    #     print(brokerID)
    #     executor.submit(cls.F)

    # @classmethod
    # def F(cls):
    #     print("Hell")
        

class BrokerMetaData:
    def __init__(self, DB_URI = '', url = '', name = '',brokerID = 0,docker_id = 0):
        self.DB_URI = DB_URI
        self.url = url
        self.docker_name = name
        self.last_beat = time.monotonic()
        self.brokerID = brokerID
        self.docker_id = docker_id
        self.lock = threading.Lock()

class Docker:
    
    cnt = 0
    # Maps Docker Name to Broker Object --> Not needed
    #self.id ={}
    lock = threading.Lock()

    @classmethod
    def build_run(cls, path:str):
        cls.lock.acquire()
        curr_id = cls.cnt
        cls.cnt += 1
        cls.lock.release()
        broker_nme = "broker" + str(curr_id)
        '''
        if(database_exists('postgresql://{}:{}@{}:{}/{}'.format(db_username,db_password,db_host,db_port,broker_nme))):
            conn = psycopg2.connect(
                user=db_username, password=db_password, host=db_host, port= db_port
            )
            conn.autocommit = True

            cursor = conn.cursor()
            sql = '''
            #DROP database {};
        '''.format(broker_nme)
            cursor.execute(sql)
            
            conn.close()
        ############## Create Database #######################
        conn = psycopg2.connect(
            user=db_username, password=db_password, host=db_host, port= db_port
        )
        conn.autocommit = True

        cursor = conn.cursor()
        sql = '''
        #CREATE database {};
        '''.format(broker_nme)
        cursor.execute(sql)
        
        conn.close()
        ####################################################
        
        '''    
        db_uri = create_postgres_db(broker_nme,broker_nme+"_db",db_username,db_password)
        #db_uri = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(db_username,db_password,db_host,db_port,broker_nme)
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
        os.system("docker rm -f broker"+str(curr_id))
        cmd = "docker run --name {} -d -p 0:5124 --expose 5124 -e DB_URI={}  --rm {}".format("broker"+str(curr_id),db_uri,docker_img_broker)
        os.system(cmd)
        print("docker run --name {} -d -p 0:5124 --expose 5124 -e DB_URI={} --rm {}".format("broker"+str(curr_id),db_uri,docker_img_broker))
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

    @classmethod
    def restartBroker(cls, brokerId):
        # TODO Ping the broker before creating one
        print("HELLL")
        oldBrokerObj:BrokerMetaData = Manager.brokers[brokerId]

        db_uri = oldBrokerObj.DB_URI

        Manager.brokers[brokerId].lock.acquire()
        # Check if the server is still dead
        print("HELLL")
        val = is_server_running(oldBrokerObj.url)
        if val:
            # Server already restarted
            Manager.brokers[brokerId].lock.release()
            return
        print("HELLL")
        docker_id = 0
        os.system("docker rm -f {}".format(oldBrokerObj.docker_name))
        os.system("docker run --name {} -d -p 0:5124 --expose 5124 -e DB_URI={} --rm {}".format(oldBrokerObj.docker_name,db_uri,docker_img_broker))
        url = get_url(oldBrokerObj.docker_name)
        url = 'http://' + url + ':5124'

        Manager.brokers[brokerId].url = url
        Manager.brokers[brokerId].docker_id = docker_id

        Manager.brokers[brokerId].lock.release()
        print("HELLL")
        with app.app_context():
            ################# DB UPDATES ########################
            BrokerMetaDataDB.query.filter_by(broker_id = brokerId).update(dict(url = url,docker_id = docker_id))
            file.write("BrokerMetaDataDB.query.filter_by(broker_id = {}).update(dict(docker_name = {},url = {},docker_id = {}))".format(brokerId,oldBrokerObj.docker_name,url,docker_id))
            db.session.commit()
            #####################################################

        return Manager.brokers[brokerId]

    def removeBroker(cls,brokerUrl):
        pass#TODO


'''
class VM:
    def __init__(self):
        self.ids = []
    #to return all vm ids
    def get(self):
        return self.ids
'''


