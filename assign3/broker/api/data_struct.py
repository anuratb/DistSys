import threading, time
from api import db
from api.models import QueueDB,Topics,Producer,Consumer
from pysyncobj import SyncObj, replicated
from pysyncobj.batteries import ReplLockManager

BROKER_ID = None

lockManager = ReplLockManager(autoUnlockTime = 75) # Lock will be released if connection dropped for more than 75 seconds
syncObj = None
QObj = None

CONLOCK = 'conlock'
PRODLOCK = 'prodlock'
MSGLOCK = 'msglock'
TOPICLOCK = 'topiclock'


class Queue(SyncObj):
    def __init__(self, topicID_, topicName_):
        self.queue = []
        # Key: Consumer ID, Value: (offset in the topic queue)
        self.Offset = {}
        self.topicID = topicID_
        self.topicName = topicName_
        self.producerList = [] # List of subscribed producers
        self.consumerList = [] # List of subscribed consumers
        
 
    def subscribeProducer(self, prodID):
        self.producerList.append(prodID)

        # TODO: check if the data is already in database
        obj = Producer(id = prodID)
        cur = Topics.query.filter_by(id = self.topicID).first()
        cur.producers.append(obj)
        db.session.add(obj)
        db.session.commit()


    def subscribeConsumer(self, conID):
        self.consumerList.append(conID)

        # TODO: check if the data is already in database
        obj = Consumer(id = conID, offset = 0)
        cur = Topics.query.filter_by(id = self.topicID).first()
        cur.consumers.append(obj)
        db.session.add(obj)
        db.session.commit()

    def addOffset(self, conID):
        self.Offset[conID] = 0

    def getUpdOffset(self, conID):
        offset = self.Offset[conID]
        if offset < len(self.queue):
            self.Offset[conID] += 1
            # TODO: check if the data is already in database
            obj = Consumer.query.filter_by(id = conID).first()
            obj.offset += 1
            db.session.commit()
        else:
            offset = -1
        return offset

    def addMessage(self, nid, msg):
        prev_id = None
        if len(self.queue):
            prev_id = self.queue[-1][0]
        self.queue.append([nid, msg])
        
        # TODO: check if the data is already in database
        obj = QueueDB(id = nid,value = msg)
        db.session.add(obj)
        topic = Topics.query.filter_by(id = self.topicID).first()
        if prev_id is None:
            topic.start_ind = nid  
        else:
            prevMsg = QueueDB.query.filter_by(id = prev_id).first()
            prevMsg.nxt_id = nid
        topic.end_ind = nid
        db.session.commit()

    def getTopicID(self):
        return self.topicID

    def getTopicName(self):
        return self.topicName

    def isConRegistered(self, conID):
        if conID in self.consumerList:
            return True
        else:
            return False
    
    def isProdRegistered(self, prodID):
        if prodID in self.producerList:
            return True
        else:
            return False

    def getMessage(self, offset):
        return self.queue[offset][1]

    def getRemainingSize(self, conID):
        return len(self.queue) - self.Offset[conID][0]

class QueueList:
    
    def __init__(self, selfAddr, otherAddrs):
        super().__init__(selfAddr, otherAddrs)
        # topicName -> Queue
        self.QList = {}
        # topicName -> Lock
        self.QLock = {}
        # ConsumerID -> Lock
        self.offsetLock = {}

        self.conID = 0
        self.prodID = 0
        self.msgID = 0
        self.topicID = 0

    @replicated
    def getNxtConID(self):
        val = self.conID
        self.conID += 1
        return val

    @replicated
    def getNxtProdID(self):
        val = self.prodID
        self.prodID += 1
        return val
    
    @replicated
    def getNxtMsgID(self):
        val = self.msgID
        self.msgID += 1
        return val
    
    @replicated
    def getNxtTopicID(self):
        val = self.topicID
        self.topicID += 1
        return val

    @replicated
    def addTopic(self, topicID, topicName, ID_LIST):
        if BROKER_ID in ID_LIST:
            # TODO: check if the data is already in database
            db.session.add(Topics(id = topicID, value = topicName))
            db.session.commit()
            self.QList[topicName] = Queue(topicID, topicName, sync = True)
            self.QLock[topicName] = threading.Lock()

    def addTopicWrapper(self, topicName, ID_LIST, topicID = None):
        self.isReady()
        while not lockManager.tryAcquire(TOPICLOCK, sync = True):
            continue
        topicID = self.getNxtTopicID(sync = True)
        lockManager.release(TOPICLOCK)
        self.addTopic(topicID, topicName, ID_LIST)
        
        return topicID

    def listTopics(self):
        self.isReady()
        return [topicName for topicName in self.QList.keys()]

    def isValidTopic(self, topicName):
        if topicName in self.QList:
            return True
        else:
            return False

    @replicated
    def addConsumer(self, topicName, conID, ID_LIST):
        if BROKER_ID in ID_LIST:
            self.QList[topicName].subscribeConsumer(topicName, conID)
            self.QList[topicName].addOffset(conID)
            self.offsetLock[conID] = threading.Lock()

    @replicated
    def addProducer(self, topicName, prodID, ID_LIST):
        if BROKER_ID in ID_LIST:
            self.QList[topicName].subscribeProducer(topicName, prodID)

    def registerConsumer(self, topicName, ID_LIST):
        self.isReady()
        if not self.isValidTopic(topicName):
            raise Exception('Topicname: {} does not exists'.format(topicName))

        while not lockManager.tryAcquire(CONLOCK, sync = True):
            continue
        nid = self.getNxtConID(sync = True)
        lockManager.release(CONLOCK)

        self.QLock[topicName].acquire()
        self.addConsumer(topicName, nid, ID_LIST, sync = True)
        self.QLock[topicName].release()

        return nid

    def registerProducer(self, topicName, ID_LIST):
        self.isReady()
        if not self.isValidTopic(topicName):
            raise Exception('Topicname: {} does not exists'.format(topicName))
        
        while not lockManager.tryAcquire(PRODLOCK, sync = True):
            continue
        nid = self.getNxtProdID(sync = True)
        lockManager.release(PRODLOCK)

        self.QLock[topicName].acquire()
        self.addProducer(topicName, nid, ID_LIST, sync = True)
        self.QLock[topicName].release()

        return nid

    @replicated
    def addMessage(self, topicName, msgID, msg, ID_LIST):
        if BROKER_ID in ID_LIST:
            self.QList[topicName].addMessage(msgID, msg, ID_LIST, sync = True)

    def enqueue(self, topicName, prodID, msg, ID_LIST):
        self.isReady()
        #print("Enqueueing: ", topicName, prodID, msg)
        #print(cls.topics.get(topicName).producerList)
      
        # Check if topic exists
        if not self.isValidTopic(topicName):
            raise Exception('Topicname: {} does not exists'.format(topicName))

        #print("----------------------")
        # Check if user is registered for the topic
        
        if not self.QList[topicName].isProdRegistered(prodID):
            raise Exception("Error: Invalid producer ID!")

        while not lockManager.tryAcquire(MSGLOCK, sync = True):
            continue
        nid = self.getNxtMsgID(sync = True)
        lockManager.release(MSGLOCK)

        self.QLock[topicName].acquire()
        self.addMessage(topicName, nid, msg, ID_LIST, sync = True)
        self.QLock[topicName].release()

    @replicated
    def getUpdOffset(self, topicName, conID, ID_LIST):
        if BROKER_ID in ID_LIST:
            index = self.QList[topicName].getUpdOffset(conID)
            return index

    def dequeue(self, topicName, conID, ID_LIST):
        self.isReady()
        # Check if topic exists
        if not self.isValidTopic(topicName):
            raise Exception('Topicname: {} does not exists'.format(topicName))
        
        # Check if user is registered for the topic
        if not self.QList[topicName].isConRegistered(conID):
            raise Exception("Error: Invalid consumer ID!")
        
        self.offsetLock[conID].acquire()
        index = self.getUpdOffset(topicName, conID, ID_LIST, sync = True)
        self.offsetLock[conID].release()

        if index == -1:
            raise Exception("There are no new messages!")
        else:
            msg = self.QList[topicName].getMessage(index)
            return msg

    def getSize(self, topicName, conID):
        self.isReady()
        if not self.isValidTopic(topicName):
            raise Exception('Topicname: {} does not exists'.format(topicName))
        # Check if user is registered for the topic
        if not self.QList[topicName].isConRegistered(conID):
            raise Exception("Error: Invalid producer ID!")
        return self.QList[topicName].getRemainingSize(conID)
        
    def isReady(self):
        while not QObj.isReady():
            time.sleep(1)
            print(f"BrokerID-{BROKER_ID}: Not ready Yet")

def createSyncObj(selfAddr, otherAddrs):
    global syncObj
    syncObj = SyncObj('localhost:%d' % selfAddr, otherAddrs, consumers = [lockManager])

def setBrokerID(broker_id):
    global BROKER_ID
    BROKER_ID = broker_id

def createQObj(selfAddr, otherAddrs):
    global QObj
    QObj = QueueList(selfAddr, otherAddrs)

def getQObj():
    return QObj
    