import threading, time
from api import db,app
from api.models import QueueDB,Topics,Producer,Consumer
from pysyncobj import SyncObj, replicated
from pysyncobj.batteries import ReplLockManager
import os
BROKER_ID = str(os.environ.get('BROKER_ID'))

lockManager = ReplLockManager(autoUnlockTime = 75) # Lock will be released if connection dropped for more than 75 seconds
syncObj = None
QObj = None

CONLOCK = 'conlock'
PRODLOCK = 'prodlock'
MSGLOCK = 'msglock'
TOPICLOCK = 'topiclock'


class Queue():
    def __init__(self, topicID_, topicName_):
        self.queue = []
        # Key: Consumer ID, Value: (offset in the topic queue)
        self.Offset = {}
        self.topicID = topicID_
        self.topicName = topicName_
        self.producerList = [] # List of subscribed producers
        self.consumerList = [] # List of subscribed consumers
        
 
    def subscribeProducer(self,topicName, prodID):
        with app.app_context():
            print("Entered subscribe producer")
            if prodID in self.producerList:
                return
            self.producerList.append(prodID)

            # TODO: check if the data is already in database
            if(len(Producer.query.filter_by(id=prodID).all())>0):
                return
            obj = Producer(id = prodID)
            cur = Topics.query.filter_by(id = self.topicID).first()
            cur.producers.append(obj)
            db.session.add(obj)
            db.session.commit()
            print("DAtabase updated")


    def subscribeConsumer(self,topicName, conID):
        with app.app_context():
            if conID in self.consumerList:
                return
            self.consumerList.append(conID)

            # TODO: check if the data is already in database
            if(len(Consumer.query.filter_by(id=conID).all())>0):
                return
            obj = Consumer(id = conID, offset = 0)
            cur = Topics.query.filter_by(id = self.topicID).first()
            cur.consumers.append(obj)
            db.session.add(obj)
            db.session.commit()

    def addOffset(self, conID):
        self.Offset[conID] = 0

    def getUpdOffset(self, conID):
        with app.app_context():
            offset = self.Offset[conID]
            if offset < len(self.queue):
                self.Offset[conID] += 1
                # TODO: check if the data is already in database

                obj = Consumer.query.filter_by(id = conID).first()
                if(obj.offset!=self.Offset[conID]):
                    obj.offset = self.Offset[conID]
                db.session.commit()
            else:
                offset = -1
            return offset

    def addMessage(self, nid, msg):
        with app.app_context():
            prev_id = None
            if len(self.queue):
                prev_id = self.queue[-1][0]
            self.queue.append([nid, msg])
            
            # TODO: check if the data is already in database
            if(len(QueueDB.query.filter_by(id=nid,value=msg).all())>0):
                return
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

class QueueList(SyncObj):
    
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

        print(BROKER_ID,ID_LIST)
        if str(BROKER_ID )in ID_LIST:
            with app.app_context():
                # TODO: check if the data is already in database
                print("Adding Topic")
                if(len(Topics.query.filter_by(id=topicID,value=topicName).all())>0):
                    return
                db.session.add(Topics(id = topicID, value = topicName))
                db.session.commit()
                self.QList[topicName] = Queue(topicID, topicName)
                self.QLock[topicName] = threading.Lock()
                print("Added Topic")

    def addTopicWrapper(self, topicName, ID_LIST, topicID = None):
        print("Entered Topic Wrapper")
        self.isReady_()
        while not lockManager.tryAcquire(TOPICLOCK, sync = True):
            continue
        print("Entered Lock Manager")
        topicID = self.getNxtTopicID(sync = True)
        print("get NExt Topic ID working")
        lockManager.release(TOPICLOCK)
        self.addTopic(topicID, topicName, ID_LIST)
        
        return topicID

    def listTopics(self):
        self.isReady_()
        
        return [topicName for topicName in self.QList.keys()]

    def isValidTopic(self, topicName):
        if topicName in self.QList:
            return True
        else:
            return False

    @replicated
    def addConsumer(self, topicName, conID, ID_LIST):
        if str(BROKER_ID) in ID_LIST:
            self.QList[topicName].subscribeConsumer(topicName, conID)
            self.QList[topicName].addOffset(conID)
            self.offsetLock[conID] = threading.Lock()

    @replicated
    def addProducer(self, topicName, prodID, ID_LIST):
        print("Add PRoducer Called")
        if str(BROKER_ID) in ID_LIST:
            self.QList[topicName].subscribeProducer(topicName, prodID)

    def registerConsumer(self, topicName, ID_LIST):
        self.isReady_()
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
        self.isReady_()
        if not self.isValidTopic(topicName):
            raise Exception('Topicname: {} does not exists'.format(topicName))
        
        while not lockManager.tryAcquire(PRODLOCK, sync = True):
            continue
        print("GG2")
        nid = self.getNxtProdID(sync = True)
        print("GG1")
        lockManager.release(PRODLOCK)

        self.QLock[topicName].acquire()
        self.addProducer(topicName, nid, ID_LIST, sync = True)
        self.QLock[topicName].release()

        return nid

    @replicated
    def addMessage(self, topicName, msgID, msg, ID_LIST):
        if str(BROKER_ID) in ID_LIST:
            self.QList[topicName].addMessage(msgID, msg)

    def enqueue(self, topicName, prodID, msg, ID_LIST):
        self.isReady_()
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
        if str(BROKER_ID) in ID_LIST:
            index = self.QList[topicName].getUpdOffset(conID)
            return index

    def dequeue(self, topicName, conID, ID_LIST):
        print("Entered Deque")
        self.isReady_()
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
        self.isReady_()
        if not self.isValidTopic(topicName):
            raise Exception('Topicname: {} does not exists'.format(topicName))
        # Check if user is registered for the topic
        if not self.QList[topicName].isConRegistered(conID):
            raise Exception("Error: Invalid producer ID!")
        return self.QList[topicName].getRemainingSize(conID)
        
    def isReady_(self):
        while not QObj.isReady():
            time.sleep(1)
            print(f"BrokerID-{BROKER_ID}: Not ready Yet")

def createSyncObj(selfAddr, otherAddrs):
    global syncObj
    syncObj = SyncObj(selfAddr, otherAddrs, consumers = [lockManager])

def setBrokerID(broker_id):
    global BROKER_ID
    BROKER_ID = broker_id

def createQObj(selfAddr, otherAddrs):
    global QObj
    QObj = QueueList(selfAddr, otherAddrs)

def getQObj():
    return QObj
    