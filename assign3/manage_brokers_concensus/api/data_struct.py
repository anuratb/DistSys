import threading
from api import db,app
import os, requests, time
from api import random,DB_URI,replFactor
from api.models import ManagerDB,TopicDB,TopicBroker,globalProducerDB,localProducerDB,globalConsumerDB,localConsumerDB,BrokerMetaDataDB
from api import db_password,db_username,docker_img_broker, APP_URL
import subprocess
from api.utils import is_server_running

from api.utils import *

from pysyncobj import SyncObj, replicated


LOG_path = './LOG.txt'

file = open('LOG_path.txt','w')

manager = None

# List of locks
topicLock = threading.Lock()
brokerLock = threading.Lock()
brokerMetaDataLock = {}
brokerTopicIDLock = threading.Lock()
msgIDLock = threading.Lock()
brokerConIDLock = threading.Lock()
brokerProdIDLock = threading.Lock()
subscriptionLock = [threading.Lock(), threading.Lock()]
rrIndexLock = [{}, {}]

class BrokerMetaData:
    def __init__(self, DB_URI = '', url = '', name = '', brokerID = 0,docker_id = 0,raft_url=''):
        self.DB_URI = DB_URI
        self.url = url
        self.docker_name = name
        self.last_beat = time.monotonic()
        self.brokerID = brokerID
        self.docker_id = docker_id
        self.raft_url = raft_url
        self.port = 8500 #initial port for raft

    
class Manager(SyncObj):

    def __init__(self, selfAddr, otherAddrs):
        super().__init__(selfAddr, otherAddrs) 
        print(selfAddr,otherAddrs)
        # Map from topicName to {topicID, numPartitions, rrindex, lock}
        self.Topics = {}
        self.TopicID = 0
        # A map from partiton#topicName to the corresponding list of brokerIDs (i.e. replica)
        self.PartitionBroker = {}
        
        # A map from broker ID to broker Metadata
        self.brokers = {}
        self.brokerID = 0
        self.Managers = {}

        # Only stores those clients that subscribe to enitre topic
        # Map from clienID#TopicName to an array X. X[i] contains an array containing brokerID followed by
        # corresponding (prodIDs, partitionNo)
        # 0 is for producer, 1 is for consumer
        self.subscription = [{}, {}]
        # Map from clientID#TopicName to round-robin index
        self.rrIndex = [{}, {}]
        self.clientID = [0, 0]

        # Broker level IDs
        self.brokerTopicID = 0
        self.msgID = 0
        self.brokerConID = 0
        self.brokerProdID = 0

    @replicated
    def Updates(self, type, data=None):
        if type == 'getBrokerProdID':
            oldVal = self.brokerProdID
            self.brokerProdID += 1
            return oldVal
        elif type=='foo':
            print( "Fooooo")
            return True
        elif type == 'getBrokerConID':
            oldVal = self.brokerConID
            self.brokerConID += 1
            return oldVal

        elif type == 'getBrokerTopicID':
            oldVal = self.brokerTopicID
            self.brokerTopicID += 1
            return oldVal

        elif type == 'getMsgID':
            oldVal = self.msgID
            self.msgID += 1
            return oldVal

        elif type == 'getTopicID':
            oldVal = self.TopicID
            self.TopicID += 1
            return oldVal

        elif type == 'getCliendID':
            isCon = data['isCon_']
            oldVal = self.clientID[isCon]
            self.clientID[isCon] += 1
            return oldVal

        elif type == 'getBrokerID':
            oldVal = self.brokerID
            self.brokerID += 1
            return oldVal

        elif type == 'addTopicRepl':
            with app.app_context():
                topicName = data['topicName_']
                topicID = data['topicID_']
                numPartitions = data['numPartitions_']
                assignedBrokers = data['assignedBrokers_']

                self.Topics[topicName] = [topicID, numPartitions, 0]
                partitionNo = 1
                for K in assignedBrokers.keys():
                    self.PartitionBroker[K] = assignedBrokers[K]

                    # TODO assignedBrokers[i] is now a list... Modify DB updates
                    ####################### DB UPDATES ###########################
                    # db.session.add(
                    #     TopicBroker(
                    #         topic = topicName,
                    #         brokerID = assignedBrokers[K],
                    #         partition = partitionNo
                    #     )
                    # )
                    # #file.write("db.session.add(TopicBroker(topic = {},brokerID = {},partition = {}))".format(topicName, assignedBrokers[K], partitionNo))
                    # partitionNo += 1
                    obj  = TopicBroker(
                        topic=topicName,
                        partition=partitionNo
                    )
                    for itr in assignedBrokers[K]:
                        obj.brokerID.append(BrokerMetaDataDB.query.filter_by(broker_id=itr).first())
                    db.session.add(obj)
                    #file.write("db.session.add(TopicBroker(topic = {},brokerID = {},partition = {}))".format(topicName, assignedBrokers[K], partitionNo))
                    partitionNo += 1
                
                db.session.commit()

                ############### DB update ###############
                obj = TopicDB(topicName = topicName, numPartitions = numPartitions, topic_id = topicID, rrindex = 0)
                #file.write("db.session.add(TopicDB(topicName={},numPartitions={},topic_id={}, rrindex=0))".format(topicName,numPartitions,topicID))
                db.session.add(obj)
                db.session.commit()
                #########################################

        elif type == 'addSubscriptionRepl':
            with app.app_context():
                clientID = data['clientID_']
                clientIDs = data['clientIDs_']
                isCon = data['isCon_']
                topicName = data['topicName_']

                K = clientID + "#" + topicName
                rrIndexLock[isCon][K] = threading.Lock()
                self.subscription[isCon][K] = clientIDs

                ################## DB Updates #####################
                glob_cli = None
                if isCon:
                    glob_cli = globalConsumerDB(glob_id = clientID, topic = topicName, rrindex = 0, brokerCnt = len(clientIDs))
                    #file.write("db.session.add(globalConsumerDB(glob_id={},topic = {},rrindex=0,brokerCnt = {}))".format(clientID,topicName,len(clientIDs)))
                else:
                    glob_cli = globalProducerDB(glob_id = clientID, topic = topicName, rrindex = 0, brokerCnt = len(clientIDs))
                    #file.write("db.session.add(globalProducerDB(glob_id={},topic = {},rrindex=0,brokerCnt = {}))".format(clientID,topicName,len(clientIDs)))
                local_clis = []
                for row in clientIDs:
                    broker_id = row[0] 
                    prod_ids = row[1:]
                    
                    for prod_id, partition in prod_ids:
                        if isCon:
                            local_clis.append(localConsumerDB(local_id = prod_id, broker_id = broker_id, glob_id = clientID, partition = partition))
                            #file.write("db.session.add(localConsumerDB(local_id = {},broker_id = {},glob_id = {},partition={}))".format(prod_id,broker_id,clientID,partition))
                        else:
                            local_clis.append(localProducerDB(local_id = prod_id, broker_id = broker_id, glob_id = clientID, partition = partition))
                            #file.write("db.session.add(localProducerDB(local_id = {},broker_id = {},glob_id = {},partition={}))".format(prod_id,broker_id,clientID,partition))
            
                cur_rrindex = random() * len(local_clis)
                glob_cli.rrindex = cur_rrindex
                db.session.add(glob_cli)
                
                for local_cli in local_clis:
                    db.session.add(local_cli)
                db.session.commit()
                ###################################################

        elif type == 'getUpdRRIndex':
            with app.app_context():
                isCon = data['isCon_']
                K = data['K_']
                topic = data['topicName_']

                if K not in self.rrIndex[isCon]:
                    self.rrIndex[isCon][K] = 0

                index = self.rrIndex[isCon][K]
                self.rrIndex[isCon][K] += 1

                # TODO DB updates
                obj = TopicDB.query.filter_by(topicName = topic).first()
                obj.rrindex = obj.rrindex + 1
                db.session.commit()

                return index

        elif type == 'addOrRestartBroker':
            with app.app_context():
                restart = data['restart_']
                curr_id = data['curr_id_']
                url = data['url_']
                docker_id = data['docker_id_']
                db_uri = data['db_uri_']
                raft_url = data['raft_url_']
                if restart:
                    self.brokers[curr_id].url = url
                    self.brokers[curr_id].docker_id = docker_id

                    with app.app_context():
                        ################# DB UPDATES ########################
                        BrokerMetaDataDB.query.filter_by(broker_id = curr_id).update(dict(url = url, docker_id = docker_id))
                        # file.write("BrokerMetaDataDB.query.filter_by(broker_id = {}).update(dict(docker_name = {},url = {},docker_id = {}))".format(curr_id, oldBrokerObj.docker_name,url,docker_id))
                        db.session.commit()
                        #####################################################
                else:
                    print("start Called",data)

                    brokerObj = BrokerMetaData(db_uri, url, "broker" + str(curr_id), curr_id, docker_id,raft_url)
                    self.brokers[brokerObj.brokerID] = brokerObj
                    brokerMetaDataLock[curr_id] = threading.Lock()
                    ################# DB UPDATES ########################
                    with app.app_context():
                        obj = BrokerMetaDataDB(
                            broker_id = brokerObj.brokerID, 
                            url = brokerObj.url,
                            db_uri = brokerObj.DB_URI,
                            docker_name = brokerObj.docker_name,
                            last_beat = brokerObj.last_beat,
                            docker_id = brokerObj.docker_id,
                            raft_url= brokerObj.raft_url

                        )
                        db.session.add(obj)
                        db.session.commit()
                    #####################################################
                    print("HASDSDSADS")





    def getMsgIDWrapper(self):
        msgIDLock.acquire()
        val = self.Updates('getMsgID',None, sync = True)
        msgIDLock.release()
        return val

    def addSubscription(self, clientIDs, topicName, isCon):
        data = {
            'isCon_': isCon
        }
        subscriptionLock[isCon].acquire()
        clientID = "$" + str(self.Updates('getCliendID', data, sync = True))
        subscriptionLock[isCon].release()
        data = {
            'clientID_': clientID,
            'clientIDs_': clientIDs,
            'isCon_': isCon,
            'topicName_': topicName
        }
        self.Updates('addSubscriptionRepl', data, sync = True)
        return clientID

    def checkSubscription(self, clientID, topicName, isCon):
        K = clientID + "#" + topicName
        if K not in self.subscription[isCon]:
            return False
        return True

    '''
    Returns the brokerID and prodID to which the message should be sent
    '''
    def getRRIndex(self, clientID, topicName, isCon):
        self.isReady_()
        K = clientID + '#' + topicName
        data = {
            'isCon_': isCon,
            'K_': K,
            'topicName_': topicName
        }
        rrIndexLock[isCon][K].acquire()
        rrIndex = self.Updates('getUpdRRIndex', data, sync = True)
        rrIndexLock[isCon][K].release()

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
        return self.getBrokerList(topicName,partition), prodID, partition


    # Adds a topic and randomly decides number of paritions
    def addTopic(self, topicName):
        numBrokers = len(self.brokers)
        val = random() * numBrokers
        numPartitions = int(val)
        if  numPartitions == 0: numPartitions = 1

        file.write("INFO Choosing {} partitions for topic {}".format(numPartitions, topicName))

        topicLock.acquire()
        if topicName in self.Topics:
            topicLock.release()
            raise Exception(f'Topicname: {topicName} already exists')
        topicID = self.Updates('getTopicID', sync = True)
        # Choose the brokers
        numPartitions, assignedBrokers = self.assignBrokers(topicName, numPartitions)
        # All Brokers are down
        if numPartitions == 0:
            raise Exception("Service currently unavailable. Please try again later.")
        self.Topics[topicName] = []
        topicLock.release()

        data = {
            'topicName_': topicName,
            'topicID_': topicID,
            'numPartitions_': numPartitions,
            'assignedBrokers_': assignedBrokers
        }
        self.Updates('addTopicRepl', data, sync = True)

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
            brokerTopicIDLock.acquire()
            brokerTopicID = self.Updates('getBrokerTopicID', sync = True)
            brokerTopicIDLock.release()
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
            #brokerTopicName = str(i) + '#' + topicName
            brokerList = self.getBrokerList(topicName,i)
            brokerID = int(random() * len(brokerList))
            brokerUrl = self.getBrokerUrlFromID(brokerList[brokerID])

            brokerCliID = None
            if not isCon:
                brokerProdIDLock.acquire()
                brokerCliID = self.Updates('getBrokerProdID', sync = True)
                brokerProdIDLock.release()
            else:
                brokerConIDLock.acquire()
                brokerCliID = self.Updates('getBrokerConID', sync = True)
                brokerConIDLock.release()

            res = requests.post(
                brokerUrl + url,
                json = {
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

    def isReady_(self):
        while not self.isReady():
            time.sleep(1)
            print(f"Updating States....")

    def build_run(self,master,slave):
        brokerLock.acquire()
        curr_id = self.Updates('getBrokerID', sync = True)
        brokerLock.release()
        
        broker_nme = "broker" + str(curr_id)
   
        db_uri = create_postgres_db(broker_nme,broker_nme+"_db",db_username,db_password)

        docker_id = 0
        print("broker"+str(curr_id),db_uri,docker_img_broker)
        os.system("docker rm -f broker"+str(curr_id))
        # cmd = "docker run --name {} -d -p 0:5124 --expose 5124 -e DB_URI={} -e BROKER_ID={}  {}".format("broker"+str(curr_id),db_uri,curr_id,docker_img_broker)
        # os.system(cmd)
        # print("docker run --name {} -d -p 0:5124 --expose 5124 -e DB_URI={}  {}".format("broker"+str(curr_id),db_uri,docker_img_broker))
        # obj = subprocess.Popen("docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' broker"+str(curr_id), shell=True, stdout=subprocess.PIPE).stdout.read()
        # url = 'http://' + obj.decode('utf-8').strip() + ':5124'
        # print(url)

        # self.addOrRestartBroker(db_uri, url, curr_id, docker_id, sync = True)
        master2 = master+":8500"
        slave_ips= ""
        for i,itr in enumerate(slave):
            if i+1==len(slave):
                slave_ips += (itr + ":8500")
            else :
                slave_ips += itr + ":8500$"

        cmd = f"docker run --net brokernet --ip {master} --name broker{curr_id} -d -p 0:5124 --expose 5124 -e CLEAR_DB={1} -e DB_URI={db_uri} -e BROKER_ID={curr_id} -e SELF_ADDR={master2} -e SLAVE_ADDR='{slave_ips}' --rm {docker_img_broker}"
        os.system(cmd)
        #print("docker run --name {} -d -p 0:5124 --expose 5124 -e DB_URI={}  {}".format("broker"+str(curr_id),db_uri,docker_img_broker))
        obj = subprocess.Popen("docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' broker"+str(curr_id), shell=True, stdout=subprocess.PIPE).stdout.read()
        url = 'http://' + obj.decode('utf-8').strip() + ':5124'
        raft_url = 'http://' + obj.decode('utf-8').strip() + ':8500'
        sync_url = 'http://' + obj.decode('utf-8').strip() + ':8501'
        print(url)

        #self.lock.acquire()
        #self.id[broker_nme] = BrokerMetaData(db_uri,url,"broker"+str(curr_id))
        #self.lock.release()
        #TopicMetaData.lock.aquire()
        #TopicMetaData.addBrokerUrl(url)
        #TopicMetaData.lock.release()
        # broker_obj = BrokerMetaData(db_uri,url,"broker"+str(curr_id),curr_id,docker_id,raft_url)
        ################# DB UPDATES ########################
        
        # obj = BrokerMetaDataDB(
        #     broker_id = broker_obj.brokerID, 
        #     url = broker_obj.url,
        #     db_uri = broker_obj.DB_URI,
        #     docker_name = broker_obj.docker_name,
        #     last_beat = broker_obj.last_beat,
        #     docker_id = broker_obj.docker_id,
        #     raft_url = raft_url,
        #     sync_url = sync_url
        # )
        # #file.write("db.session.add(BrokerMetaDataDB(broker_id = {}, url = {},db_uri = {},docker_name = {},last_beat = {},docker_id = {}))".format(broker_obj.brokerID, broker_obj.url,broker_obj.DB_URI,broker_obj.docker_name,broker_obj.last_beat,broker_obj.docker_id))
        # db.session.add(obj)
        # db.session.commit()
        #####################################################
        data = {
            'restart_': False,
            'curr_id_': curr_id,
            'url_': url,
            'docker_id_': docker_id,
            'db_uri_': db_uri,
            'raft_url_' :raft_url
        }
        self.Updates(
            'addOrRestartBroker', data,
            sync=True
        )

    def restartBroker(self, brokerId):
        if not is_leader(): return
        print("HELLL")
        oldBrokerObj:BrokerMetaData = self.brokers[brokerId]

        db_uri = oldBrokerObj.DB_URI

        brokerMetaDataLock[brokerId].acquire()
        # Check if the server is still dead
        print("HELLL")
        val = is_server_running(oldBrokerObj.url)
        if val:
            # Server already restarted
            brokerMetaDataLock[brokerId].release()
            return
        print("HELLL")
        docker_id = 0
        os.system("docker rm -f {}".format(oldBrokerObj.docker_name))
        os.system("docker run --name {} -d -p 0:5124 --expose 5124 -e DB_URI={} --rm {}".format(oldBrokerObj.docker_name,db_uri,docker_img_broker))
        url = get_url(oldBrokerObj.docker_name)
        url = 'http://' + url + ':5124'
        data = {
            'restart_': True,
            'curr_id_': brokerId,
            'url_': url,
            'docker_id_': docker_id,
            'db_uri_': db_uri
        }
        self.Updates('addOrRestartBroker', data, sync = True)
        brokerMetaDataLock[brokerId].release()
        print("HELLL")

    def restartManager(self, managerId):
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
                #print(brokerMetaDataLock.keys())
                brokerMetaDataLock[broker.docker_id].acquire()
                # broker.lock.acquire()
                broker.last_beat = time.monotonic()
                # broker.lock.release()
                brokerMetaDataLock[broker.docker_id].release()
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