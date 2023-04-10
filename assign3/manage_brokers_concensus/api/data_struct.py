import threading
from . import db,app
import os, requests, time
from api import random,DB_URI,replFactor
from api.models import ManagerDB,ReplicationDB, TopicDB,TopicBroker,globalProducerDB,localProducerDB,globalConsumerDB,localConsumerDB,BrokerMetaDataDB
from api import db_password,db_username,docker_img_broker, APP_URL
import subprocess
from api.utils import is_server_running

from api.utils import *

from pysyncobj import SyncObj, replicated


LOG_path = './LOG.txt'

file = open('LOG_path.txt','w')

manager = None

class BrokerMetaData:
    def __init__(self, DB_URI = '', url = '', name = '', brokerID = 0,docker_id = 0):
        self.DB_URI = DB_URI
        self.url = url
        self.docker_name = name
        self.last_beat = time.monotonic()
        self.brokerID = brokerID
        self.docker_id = docker_id
        self.lock = threading.Lock()
        self.port = 8000 #initial port for raft
    
class Manager(SyncObj):

    def __init__(self, selfAddr, otherAddrs):
        super().__init__(selfAddr, otherAddrs) 
        # Map from topicName to {topicID, numPartitions, rrindex, lock}
        self.Topics = {}
        self.TopicID = 0
        # A map from partiton#topicName to the corresponding list of brokerIDs (i.e. replica)
        self.PartitionBroker = {}
        self.lock = threading.Lock()
        
        # A map from broker ID to broker Metadata
        self.brokers = {}
        self.brokerID = 0
        self.brokerLock = threading.Lock()
        self.Managers = {}

        # Only stores those clients that subscribe to enitre topic
        # Map from clienID#TopicName to an array X. X[i] contains an array containing brokerID followed by
        # corresponding (prodIDs, partitionNo)
        # 0 is for producer, 1 is for consumer
        self.subscription = [{}, {}]
        # Map from clientID#TopicName to round-robin index
        self.rrIndex = [{}, {}]
        self.clientID = [0, 0]
        self.subscriptionLock = [threading.Lock(), threading.Lock()]
        self.rrIndexLock = [{}, {}]

        # Broker level IDs
        self.brokerTopicID = 0
        self.brokerTopicIDLock = threading.Lock()
        self.msgID = 0
        self.msgIDLock = threading.Lock()
        self.brokerConID = 0
        self.brokerConIDLock = threading.Lock()
        self.brokerProdID = 0
        self.brokerProdIDLock = threading.Lock()

    @replicated
    def getBrokerProdID(self):
        self.brokerProdIDLock.acquire()
        oldVal = self.brokerProdID
        self.brokerProdID += 1
        self.brokerProdIDLock.release()
        return oldVal

    @replicated
    def getBrokerConID(self):
        self.brokerConIDLock.acquire()
        oldVal = self.brokerConID
        self.brokerConID += 1
        self.brokerConIDLock.release()
        return oldVal

    @replicated
    def getBrokerTopicID(self):
        self.brokerTopicIDLock.acquire()
        oldVal = self.brokerTopicID
        self.brokerTopicID += 1
        self.brokerTopicIDLock.release()
        return oldVal

    @replicated
    def getMsgID(self):
        self.msgIDLock.acquire()
        oldVal = self.msgID
        self.msgID += 1
        self.msgIDLock.release()
        return oldVal

    @replicated
    def getTopicID(self):
        oldVal = self.TopicID
        self.TopicID += 1
        return oldVal

    @replicated
    def getCliendID(self, isCon):
        oldVal = self.clientID[isCon]
        self.clientID[isCon] += 1
        return oldVal

    @replicated
    def getBrokerID(self):
        oldVal = self.brokerID
        self.brokerID += 1
        return oldVal

    @replicated
    def addBroker(self, db_uri, url, curr_id, docker_id):
        brokerObj = BrokerMetaData(db_uri, url, "broker" + str(curr_id), curr_id, docker_id)
        self.brokers[brokerObj.brokerID] = brokerObj
        ################# DB UPDATES ########################
        obj = BrokerMetaDataDB(
            broker_id = brokerObj.brokerID, 
            url = brokerObj.url,
            db_uri = brokerObj.DB_URI,
            docker_name = brokerObj.docker_name,
            last_beat = brokerObj.last_beat,
            docker_id = brokerObj.docker_id,
        )
        file.write("db.session.add(BrokerMetaDataDB(broker_id = {}, url = {},db_uri = {},docker_name = {},last_beat = {},docker_id = {}))".format(broker_obj.brokerID, broker_obj.url,broker_obj.DB_URI,broker_obj.docker_name,broker_obj.last_beat,broker_obj.docker_id))
        db.session.add(obj)
        db.session.commit()
        #####################################################

        return brokerObj

    @replicated
    def addTopicRepl(self, topicID, topicName, numPartitions, assignedBrokers):
        self.Topics[topicName] = [topicID, numPartitions, 0, threading.Lock()]
        partitionNo = 1
        for K in assignedBrokers.keys():
            self.PartitionBroker[K] = assignedBrokers[K]

            # TODO assignedBrokers[i] is now a list... Modify DB updates
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

        ############### DB update ###############
        obj = TopicDB(topicName = topicName, numPartitions = numPartitions, topic_id = topicID, rrindex = 0)
        file.write("db.session.add(TopicDB(topicName={},numPartitions={},topic_id={}, rrindex=0))".format(topicName,numPartitions,topicID))
        db.session.add(obj)
        db.session.commit()
        #########################################

    @replicated
    def addSubscriptionRepl(self, clientID, topicName, clientIDs, isCon):
        K = clientID + "#" + topicName
        self.rrIndex[isCon][K] = 0
        self.rrIndexLock[isCon][K] = threading.Lock()
        self.subscription[isCon][K] = clientIDs

        ################## DB Updates #####################
        glob_cli = None
        if isCon:
            glob_cli = globalConsumerDB(glob_id = clientID, topic = topicName, rrindex = 0, brokerCnt = len(clientIDs))
            file.write("db.session.add(globalConsumerDB(glob_id={},topic = {},rrindex=0,brokerCnt = {}))".format(clientID,topicName,len(clientIDs)))
        else:
            glob_cli = globalProducerDB(glob_id = clientID, topic = topicName, rrindex = 0, brokerCnt = len(clientIDs))
            file.write("db.session.add(globalProducerDB(glob_id={},topic = {},rrindex=0,brokerCnt = {}))".format(clientID,topicName,len(clientIDs)))
        local_clis = []
        for row in clientIDs:
            broker_id = row[0] 
            prod_ids = row[1:]
            
            for prod_id, partition in prod_ids:
                if isCon:
                    local_clis.append(localConsumerDB(local_id = prod_id, broker_id = broker_id, glob_id = clientID, partition = partition))
                    file.write("db.session.add(localConsumerDB(local_id = {},broker_id = {},glob_id = {},partition={}))".format(prod_id,broker_id,clientID,partition))
                else:
                    local_clis.append(localProducerDB(local_id = prod_id, broker_id = broker_id, glob_id = clientID, partition = partition))
                    file.write("db.session.add(localProducerDB(local_id = {},broker_id = {},glob_id = {},partition={}))".format(prod_id,broker_id,clientID,partition))
    
        cur_rrindex = random() * len(local_clis)
        glob_cli.rrindex = cur_rrindex
        db.session.add(glob_cli)
        
        for local_cli in local_clis:
            db.session.add(local_cli)
        db.session.commit()
        ###################################################

    def addSubscription(self, clientIDs, topicName, isCon):
        self.subscriptionLock[isCon].acquire()
        clientID = "$" + str(self.getCliendID(isCon, sync = True))
        self.subscriptionLock[isCon].release()
       
        self.addSubscriptionRepl(clientID, topicName, clientIDs, isCon, sync = True)
        return clientID

    def checkSubscription(self, clientID, topicName, isCon):
        K = clientID + "#" + topicName
        if K not in self.subscription[isCon]:
            return False
        return True

    @replicated
    def getUpdRRIndex(self, K, topic, isCon):
        index = self.rrIndex[isCon][K]
        self.rrIndex[isCon][K] += 1

        # TODO DB updates
        obj = TopicDB.query.filter_by(topicName = topic).first()
        obj.rrindex = obj.rrindex + 1            
        db.session.commit()

        return index
    '''
    Returns the brokerID and prodID to which the message should be sent
    '''
    def getRRIndex(self, clientID, topicName, isCon):
        K = clientID + '#' + topicName
        self.rrIndexLock[isCon][K].acquire()
        rrIndex = self.getUpdRRIndex(K, isCon, topicName, sync = True)
        self.rrIndexLock[isCon][K].release()

        globCli = None
        if isCon:
            globCli = globalConsumerDB.query.filter_by(glob_id = clientID, topic = topicName).first()
        else:
            globCli = globalProducerDB.query.filter_by(glob_id = clientID, topic = topicName).first()

        # To ensure load balancing
        localClis = None
        if isCon:
            localClis = globCli.localConsumer
        else:
            localClis = globCli.localProducer
        cnt = len(localClis)
        ind = (rrIndex + int(globCli.rrindex)) % cnt

        localCli = localClis[ind]
        prodID = localCli.local_id
        partition = localCli.partition
        subbedTopicName = self.getSubbedTopicName(topicName, partition)

        return self.getBrokerList(subbedTopicName), prodID, partition


    # Adds a topic and randomly decides number of paritions
    def addTopic(self, topicName):
        numBrokers = len(self.brokers)
        val = random() * numBrokers
        numPartitions = int(val)
        if  numPartitions == 0: numPartitions = 1

        file.write("INFO Choosing {} partitions for topic {}".format(numPartitions, topicName))

        self.lock.acquire()
        if topicName in self.Topics:
            self.lock.release()
            raise Exception(f'Topicname: {topicName} already exists')
        topicID = self.getTopicID(sync = True)
        # Choose the brokers
        numPartitions, assignedBrokers = self.assignBrokers(topicName, numPartitions)
        # All Brokers are down
        if numPartitions == 0:
            raise Exception("Service currently unavailable. Please try again later.")
        self.Topics[topicName] = []
        self.lock.release()

        self.addTopicRepl(topicID, topicName, numPartitions, assignedBrokers, sync = True)


    def getRandomBrokers(self):
        # Return a random set of replfactor many brokerIDs
        brokerSet = []
        remBrokerIDs = list(self.brokers.keys())
        l = len(remBrokerIDs)
        for _ in range(replFactor):
            randInd = int(random() * l)
            brokerSet.append(remBrokerIDs[randInd])
            remBrokerIDs.remove(remBrokerIDs[randInd])
            l -= 1
        return brokerSet


    def assignBrokers(self, topicName, numPartitions):
        PartitionBroker = {}
        actualPartitions = 0

        # TODO Perform necessary DB updates here
        for _ in range(numPartitions):
            # TODO assign the broker url
            brokerSet = self.getRandomBrokers()
            brokerTopicName = str(actualPartitions + 1) + '#' + topicName

            url = self.brokers[brokerSet[0]].url
            brokerTopicID = self.getBrokerTopicID(sync = True)
            # Send Post request to any one of the replicas
            res = requests.post(str(url) + "/topics", 
                json = {
                    "name": brokerTopicName,
                    "ID_LIST": brokerSet,
                    "topicID": brokerTopicID
                }
            )
            # Broker failure
            if res.status_code != 200:
                requests.get(APP_URL + "/crash_recovery", params = {'brokerID': str(brokerSet[0])})
            elif(res.json().get("status") == "Success"):
                PartitionBroker[brokerTopicName] = brokerSet
                actualPartitions += 1    
        return actualPartitions, PartitionBroker


    def registerClientForAllPartitions(self, url, topicName, isCon):
        if topicName not in self.Topics.keys():
            raise Exception(f"Topic {topicName} doesn't exist")

        numPartitions = self.Topics[topicName][1]
        IDs = []
        brokerMap = {}
        for i in range(1, numPartitions + 1):
            brokerTopicName = str(i) + '#' + topicName
            brokerList = self.getBrokerList(brokerTopicName)
            brokerID = int(random() * len(brokerList))
            brokerUrl = self.getBrokerUrlFromID(brokerList[brokerID])

            brokerCliID = None
            if not isCon:
                brokerCliID = self.getBrokerProdID()
            else:
                brokerCliID = self.getBrokerConID()

            res = requests.post(
                brokerUrl + url,
                json={
                    "topic": topicName,
                    "partition": str(i),
                    "ID_LIST": brokerList,
                    "ID": brokerCliID
                })

            # TODO Broker Failure
            if res.status_code != 200:
                requests.get(APP_URL + "/crash_recovery", params = {'brokerID': str(brokerID)})
            elif(res.json().get("status") != "Success"):
                raise Exception(res.json().get("message"))
            else:
                ID = None
                if not isCon:
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

        return self.addSubscription(IDs, topicName, isCon)


    def build_run(self):
        self.brokerLock.acquire()
        curr_id = getManager().getBrokerID()
        self.brokerLock.release()
        
        broker_nme = "broker" + str(curr_id)
   
        db_uri = create_postgres_db(broker_nme,broker_nme+"_db",db_username,db_password)

        docker_id = 0
        print("broker"+str(curr_id),db_uri,docker_img_broker)
        os.system("docker rm -f broker"+str(curr_id))
        cmd = "docker run --name {} -d -p 0:5124 --expose 5124 -e DB_URI={} -e BROKER_ID={}  {}".format("broker"+str(curr_id),db_uri,curr_id,docker_img_broker)
        os.system(cmd)
        print("docker run --name {} -d -p 0:5124 --expose 5124 -e DB_URI={}  {}".format("broker"+str(curr_id),db_uri,docker_img_broker))
        obj = subprocess.Popen("docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' broker"+str(curr_id), shell=True, stdout=subprocess.PIPE).stdout.read()
        url = 'http://' + obj.decode('utf-8').strip() + ':5124'
        print(url)

        getManager().addBroker(db_uri, url, curr_id, docker_id, sync = True)

    @replicated
    def restartBrokerRepl(self, url, brokerId, docker_id):
        self.brokers[brokerId].url = url
        self.brokers[brokerId].docker_id = docker_id

        with app.app_context():
            ################# DB UPDATES ########################
            BrokerMetaDataDB.query.filter_by(broker_id = brokerId).update(dict(url = url,docker_id = docker_id))
            file.write("BrokerMetaDataDB.query.filter_by(broker_id = {}).update(dict(docker_name = {},url = {},docker_id = {}))".format(brokerId,oldBrokerObj.docker_name,url,docker_id))
            db.session.commit()
            #####################################################

    def restartBroker(self, brokerId):
        if not is_leader(): return
        print("HELLL")
        oldBrokerObj:BrokerMetaData = self.brokers[brokerId]

        db_uri = oldBrokerObj.DB_URI

        self.brokers[brokerId].lock.acquire()
        # Check if the server is still dead
        print("HELLL")
        val = is_server_running(oldBrokerObj.url)
        if val:
            # Server already restarted
            self.brokers[brokerId].lock.release()
            return
        print("HELLL")
        docker_id = 0
        os.system("docker rm -f {}".format(oldBrokerObj.docker_name))
        os.system("docker run --name {} -d -p 0:5124 --expose 5124 -e DB_URI={} --rm {}".format(oldBrokerObj.docker_name,db_uri,docker_img_broker))
        url = get_url(oldBrokerObj.docker_name)
        url = 'http://' + url + ':5124'

        self.restartBrokerRepl(url, brokerId, docker_id, sync = True)

        self.brokers[brokerId].lock.release()
        print("HELLL")

    @classmethod
    def restartManager(cls, managerId):
        # TODO Ping the broker before creating one
        print("HELLL")
        oldManagerObj = ManagerDB.query.filter_by(id = managerId).first()

        db_uri = DB_URI #sharing database 
             

        # Check if the server is still dead
        print("HELLL")
        val = is_server_running(oldManagerObj.url)
        if val:
            # Server already restarted
            
            return
        print("HELLL")
        docker_id = 0
        os.system("docker rm -f {}".format(managerId))
        url = create_container(db_uri,managerId,os.environ['DOCKER_IMG_MANAGER'],envs={
            'IS_WRITE':'0'
        }) 

       
        print("HELLL")
        with app.app_context():
            ################# DB UPDATES ########################
            oldManagerObj.url = url
            db.session.commit()
            #####################################################

    def removeBroker(cls,brokerUrl):
        pass#TODO

    def getBrokerUrlFromID(self, brokerID):
        #To be done by read only manager
        return BrokerMetaDataDB.query.filter_by(broker_id = brokerID).first().url
        #return cls.brokers[brokerID].url


    def checkBrokerHeartBeat(self):
        if not is_leader(): return
        for _, broker in self.brokers.items():
            val = is_server_running(broker.url)
            if val:
                broker.lock.acquire()
                broker.last_beat = time.monotonic()
                broker.lock.release()
            else:
                requests.get(APP_URL + "/crash_recovery", params = {'brokerID': str(broker.brokerID)})

    def checkManagerHeartBeat(self):
        if not is_leader(): return
        with app.app_context():
            for obj in ManagerDB.query.all():
                val = is_server_running(obj.url)
                if val:
                    pass
                else:
                    requests.get(APP_URL + "/crash_recovery_manager", params = {'managerID': str(obj.id)})

    def getSubbedTopicName(self, topicName, partition = 0):
        if topicName not in self.Topics:
            raise Exception(f"Topicname: {topicName} doesn't exist")
        ret = str(partition) + '#' + topicName
        if partition:
            if ret not in self.PartitionBroker:
                raise Exception(f"Invalid partition for topic: {topicName}")
        return ret


    def getBrokerList(self, topicName, partition):
        subbedTopicName = self.getSubbedTopicName(topicName, partition)
        return self.PartitionBroker[subbedTopicName]

    def getTopicsList(self):
        return list(self.Topics.keys())


def setManager(selfAddr, otherAddr):
    global manager
    manager = Manager(selfAddr, otherAddr)

def getManager() -> Manager:
    return manager

def get_status():
    status = getManager().getStatus()
    status['self'] = status['self'].address

    if status['leader']:
        status['leader'] = status['leader'].address

    serializable_status = {
        **status,
        'is_leader': status['self'] == status['leader'],
    }
    return serializable_status


def is_leader():
    return get_status().get('is_leader', False)