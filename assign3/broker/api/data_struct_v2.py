import threading
from api import db
from api.models import QueueDB,Topics,Producer,Consumer
from pysyncobj import SyncObj, replicated
from pysyncobj.batteries import ReplLockManager

lockManager = ReplLockManager(autoUnlockTime = 75) # Lock will be released if connection dropped for more than 75 seconds
syncObj = None
idSet = None

CONLOCK = 'conlock'
PRODLOCK = 'prodlock'
MSGLOCK = 'msglock'
TOPICLOCK = 'topiclock'

class IDSet:
    def __init__(self, selfAddr, otherAddrs, condID_ = 0, prodID_ = 0, msgID_ = 0, topicID_ = 0):
        super().__init__(selfAddr, otherAddrs)
        self.conID = condID_
        self.prodID = prodID_
        self.msgID = msgID_
        self.topicID = topicID_
    
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

def createSyncObj(selfAddr, otherAddrs):
    global syncObj
    syncObj = SyncObj('localhost:%d' % selfAddr, otherAddrs, consumers = [lockManager])

def createIDSet(selfAddr, otherAddrs, condID_ = 0, prodID_ = 0, msgID_ = 0, topicID_ = 0):
    global idSet
    idSet = IDSet('localhost:%d' % selfAddr, otherAddrs, condID_, prodID_, msgID_, topicID_)


class Queue(SyncObj):
    def __init__(self, selfAddr, otherAddrs):
        super().__init__(selfAddr, otherAddrs)
        self.queue = []
        # Key: Consumer ID, Value: (offset in the topic queue, Lock)
        self.Offset = {}
        self.topicID = None
        self.producerList = [] # List of subscribed producers
        self.plock = threading.Lock() # Lock for producerList
        self.consumerList = [] # List of subscribed consumers
        self.clock = threading.Lock() # Lock for consumerList

    @replicated
    def setTopic(self, topicID_, topicName_):
        # TODO: check if the data is already in database
        db.session.add(Topics(id = topicID_, value = topicName_))
        db.session.commit()
        self.topicID = topicID_
        self.topicName = topicName_

    @replicated  
    def subscribeProducer(self, prodID):
        self.producerList.append(prodID)

        # TODO: check if the data is already in database
        obj = Producer(id = prodID)
        cur = Topics.query.filter_by(id = self.topicID).first()
        cur.producers.append(obj)
        db.session.add(obj)
        db.session.commit()

    @replicated
    def subscribeConsumer(self, conID):
        self.consumerList.append(conID)

        # TODO: check if the data is already in database
        obj = Consumer(id = conID, offset = 0)
        cur = Topics.query.filter_by(id = self.topicID).first()
        cur.consumers.append(obj)
        db.session.add(obj)
        db.session.commit()

    @replicated
    def addOffset(self, conID):
        self.Offset[conID] = [0, threading.Lock()]

    @replicated
    def getUpdOffset(self, conID):
        offset = self.Offset[conID][0]
        if offset < len(self.queue):
            self.Offset[conID][0] += 1
            # TODO: check if the data is already in database
            obj = Consumer.query.filter_by(id = conID).first()
            obj.offset += 1
            db.session.commit()
        else:
            offset = -1
        return offset

    @replicated
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
    # topicName -> Queue
    QList = {}
    # topicName -> Lock
    QLock = {}
    # ConsumerID -> Lock
    offsetLock = {}
        
    @classmethod
    def createTopic(cls, selfAddr, otherAddrs, topicName):
        node = Queue(selfAddr, otherAddrs)
        cls.QList[topicName] = node
        cls.QLock[topicName] = threading.Lock()

    @classmethod
    def addTopic(cls, topicName, topicID = None):
        while not lockManager.tryAcquire(TOPICLOCK, sync = True):
            continue
        topicID = idSet.getNxtTopicID(sync = True)
        lockManager.release(TOPICLOCK)

        cls.QList[topicName].setTopic(topicID, topicName, sync = True)
        return topicID

    @classmethod
    def listTopics(cls):
        return [topicName for topicName in cls.QList.keys()]

    @classmethod
    def isValidTopic(cls, topicName):
        if topicName in cls.QList:
            return True
        else:
            return False

    @classmethod
    def addConsumer(cls, topicName, conID):
        cls.QLock[topicName].acquire()
        cls.QList[topicName].subscribeConsumer(conID, sync = True)
        cls.QLock[topicName].release()


    @classmethod
    def addProducer(cls, topicName, prodID):
        cls.QLock[topicName].acquire()
        cls.QList[topicName].subscribeProducer(prodID, sync = True)
        cls.QLock[topicName].release()

    @classmethod
    def registerConsumer(cls, topicName):
        if not cls.isValidTopic(topicName):
            raise Exception('Topicname: {} does not exists'.format(topicName))

        while not lockManager.tryAcquire(CONLOCK, sync = True):
            continue
        nid = idSet.getNxtConID(sync = True)
        lockManager.release(CONLOCK)

        cls.addConsumer(topicName, nid)
        cls.QList[topicName].addOffset(nid, sync = True)

        return nid

    @classmethod
    def registerProducer(cls, topicName):
        if not cls.isValidTopic(topicName):
            raise Exception('Topicname: {} does not exists'.format(topicName))
        
        while not lockManager.tryAcquire(PRODLOCK, sync = True):
            continue
        nid = idSet.getNxtProdID(sync = True)
        lockManager.release(PRODLOCK)

        cls.addProducer(topicName, nid)

        return nid

    @classmethod
    def enqueue(cls, topicName, prodID, msg):
        
        #print("Enqueueing: ", topicName, prodID, msg)
        #print(cls.topics.get(topicName).producerList)
      
        # Check if topic exists
        if not cls.isValidTopic(topicName):
            raise Exception('Topicname: {} does not exists'.format(topicName))

        #print("----------------------")
        # Check if user is registered for the topic
        
        if not cls.QList[topicName].isProdRegistered(prodID):
            raise Exception("Error: Invalid producer ID!")

        while not lockManager.tryAcquire(MSGLOCK, sync = True):
            continue
        nid = idSet.getNxtMsgID(sync = True)
        lockManager.release(MSGLOCK)

        cls.QLock[topicName].acquire()
        cls.QList[topicName].addMessage(nid, msg, sync = True)
        cls.QLock[topicName].release()

    @classmethod
    def dequeue(cls, topicName, conID):
        # Check if topic exists
        if not cls.isValidTopic(topicName):
            raise Exception('Topicname: {} does not exists'.format(topicName))
        
        # Check if user is registered for the topic
        if not cls.QList[topicName].isConRegistered(conID):
            raise Exception("Error: Invalid consumer ID!")
        
        cls.QLock[topicName].acquire()
        index = cls.QList[topicName].getUpdOffset(conID)
        cls.QLock[topicName].release()

        if index == -1:
            raise Exception("There are no new messages!")
        else:
            msg = cls.QList[topicName].getMessage(index)
            return msg

    @classmethod
    def getSize(cls, topicName, conID):
        if not cls.isValidTopic(topicName):
            raise Exception('Topicname: {} does not exists'.format(topicName))
        # Check if user is registered for the topic
        if not cls.QList[topicName].isConRegistered(conID):
            raise Exception("Error: Invalid producer ID!")
        return cls.QList[topicName].getRemainingSize(conID)
        
        