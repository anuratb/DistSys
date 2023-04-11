import threading, time
from api import db,app
from api.models import QueueDB,Topics,Producer,Consumer
from pysyncobj import SyncObj, replicated
from pysyncobj.batteries import ReplLockManager
import os
BROKER_ID = str(os.environ.get('BROKER_ID'))

QObj = None


class QueueList(SyncObj):
    
    def __init__(self, selfAddr, otherAddrs):
        super().__init__(selfAddr, otherAddrs)
        # topicName -> Lock
        self.QLock = {}
        # ConsumerID -> Lock
        self.offsetLock = {}
        # topicName to list of mssges
        self.queue = {}
        # Key: Consumer ID, Value: (offset in the topic queue)
        self.Offset = {}
        # topicName -> topicID
        self.topics = {}
        # topicName -> List of subscribed producers
        self.producerList = {}
        # topicName -> List of subscribed consumers
        self.consumerList = {} 

    @replicated
    def addTopic(self, topicID, topicName, ID_LIST):
        print(BROKER_ID,ID_LIST)
        if str(BROKER_ID) in ID_LIST:
            with app.app_context():
                # TODO: check if the data is already in database
                print("Adding Topic")
                if(len(Topics.query.filter_by(id=topicID,value=topicName).all())>0):
                    return
                db.session.add(Topics(id = topicID, value = topicName))
                db.session.commit()
                self.topics[topicName] = topicID
                self.QLock[topicName] = threading.Lock()
                print("Added Topic")


    def addTopicWrapper(self, topicName, ID_LIST, topicID_):
        print("Entered Topic Wrapper")
        self.isReady_()
        print("Entered Lock Manager")
        topicID = topicID_
        print("get NExt Topic ID working")
        self.addTopic(topicID, topicName, ID_LIST, sync = True)
        
        return topicID

    def listTopics(self):
        self.isReady_()
        
        return [topicName for topicName in self.topics.keys()]

    def isValidTopic(self, topicName):
        if topicName in self.topics:
            return True
        else:
            return False

    @replicated
    def addConsumer(self, topicName, conID, ID_LIST):
        if BROKER_ID in ID_LIST:
            if topicName not in self.consumerList:
                self.consumerList[topicName] = []
            self.consumerList[topicName].append(conID)
            self.offsetLock[conID] = threading.Lock()

            # TODO: check if the data is already in database
            if(len(Consumer.query.filter_by(id = conID).all()) > 0):
                return
            obj = Consumer(id = conID, offset = 0)
            cur = Topics.query.filter_by(id = self.topics[topicName]).first()
            cur.consumers.append(obj)
            db.session.add(obj)
            db.session.commit()

            # self.QList[topicName].subscribeConsumer(topicName, conID)
            # self.QList[topicName].addOffset(conID)
            # self.offsetLock[conID] = threading.Lock()

    @replicated
    def addProducer(self, topicName, prodID, ID_LIST):
        if BROKER_ID in ID_LIST:
            if topicName not in self.producerList:
                self.producerList[topicName] = []
            self.producerList[topicName].append(prodID)
            self.offsetLock[prodID] = threading.Lock()

            # TODO: check if the data is already in database
            if(len(Producer.query.filter_by(id = prodID).all()) > 0):
                return
            obj = Producer(id = prodID)
            cur = Topics.query.filter_by(id = self.topics[topicName]).first()
            cur.producers.append(obj)
            db.session.add(obj)
            db.session.commit()

    def registerConsumer(self, topicName, ID_LIST, conID):
        self.isReady_()
        if not self.isValidTopic(topicName):
            raise Exception('Topicname: {} does not exists'.format(topicName))


        nid = conID
        self.QLock[topicName].acquire()
        self.addConsumer(topicName, nid, ID_LIST, sync = True)
        self.QLock[topicName].release()

        return nid

    def registerProducer(self, topicName, ID_LIST, prodID):
        self.isReady_()
        if not self.isValidTopic(topicName):
            raise Exception('Topicname: {} does not exists'.format(topicName))
        
        nid = prodID

        self.QLock[topicName].acquire()
        self.addProducer(topicName, nid, ID_LIST, sync = True)
        self.QLock[topicName].release()

        return nid


    @replicated
    def addMessage(self, topicName, msgID, msg, ID_LIST):
        if BROKER_ID in ID_LIST:
            if topicName not in self.queue:
                self.queue[topicName] = []
            prev_id = None
            if len(self.queue):
                prev_id = self.queue[-1][0]
            self.queue.append([msgID, msg])
            
            # TODO: check if the data is already in database
            if(len(QueueDB.query.filter_by(id = msgID, value = msg).all()) > 0):
                return
            obj = QueueDB(id = msgID, value = msg)
            db.session.add(obj)
            topic = Topics.query.filter_by(id = self.topics[topicName]).first()
            if prev_id is None:
                topic.start_ind = msgID  
            else:
                prevMsg = QueueDB.query.filter_by(id = prev_id).first()
                prevMsg.nxt_id = msgID
            topic.end_ind = msgID
            db.session.commit()

    def enqueue(self, topicName, prodID, msg, ID_LIST, msgID):
        self.isReady_()
        #print("Enqueueing: ", topicName, prodID, msg)
        #print(cls.topics.get(topicName).producerList)
      
        # Check if topic exists
        if not self.isValidTopic(topicName):
            raise Exception('Topicname: {} does not exists'.format(topicName))

        #print("----------------------")
        # Check if user is registered for the topic
        
        if not self.isProdRegistered(topicName, prodID):
            raise Exception("Error: Invalid producer ID!")

        nid = msgID

        self.QLock[topicName].acquire()
        self.addMessage(topicName, nid, msg, ID_LIST, sync = True)
        self.QLock[topicName].release()

    @replicated
    def getUpdOffset(self, topicName, conID, ID_LIST):
        if BROKER_ID in ID_LIST:
            if conID not in self.Offset:
                self.Offset[conID] = 0
                # TODO DB update for offset creation
                return
            offset = self.Offset[conID]
            if offset < len(self.queue[topicName]):
                self.Offset[conID] += 1
                # TODO: check if the data is already in database
                obj = Consumer.query.filter_by(id = conID).first()
                obj.offset += 1
                db.session.commit()
            else:
                offset = -1
            return offset

    def dequeue(self, topicName, conID, ID_LIST):
        self.isReady_()
        # Check if topic exists
        if not self.isValidTopic(topicName):
            raise Exception('Topicname: {} does not exists'.format(topicName))
        
        # Check if user is registered for the topic
        if not self.isConRegistered(topicName, conID):
            raise Exception("Error: Invalid consumer ID!")
        
        self.offsetLock[conID].acquire()
        index = self.getUpdOffset(topicName, conID, ID_LIST, sync = True)
        self.offsetLock[conID].release()

        if index == -1:
            raise Exception("There are no new messages!")
        else:
            msg = self.getMessage(topicName, index)
            return msg

    def getSize(self, topicName, conID):
        self.isReady_()
        if not self.isValidTopic(topicName):
            raise Exception('Topicname: {} does not exists'.format(topicName))
        # Check if user is registered for the topic
        if not self.isConRegistered(topicName, conID):
            raise Exception("Error: Invalid producer ID!")
        return self.getRemainingSize(topicName, conID)
        
    def isReady_(self):
        while not QObj.isReady():
            time.sleep(1)
            print(f"BrokerID-{BROKER_ID}: Not ready Yet")

    def getTopicID(self, topicName):
        return self.topics[topicName]

    def isConRegistered(self, topicName, conID):
        if conID in self.consumerList[topicName]:
            return True
        else:
            return False
    
    def isProdRegistered(self, topicName, prodID):
        if prodID in self.producerList[topicName]:
            return True
        else:
            return False

    def getMessage(self, topicName, offset):
        return self.queue[topicName][offset][1]

    def getRemainingSize(self, topicName, conID):
        return len(self.queue[topicName]) - self.Offset[conID]

def setBrokerID(broker_id):
    global BROKER_ID
    BROKER_ID = broker_id

def createQObj(selfAddr, otherAddrs):
    global QObj
    QObj = QueueList(selfAddr, otherAddrs)

def getQObj() -> QueueList:
    return QObj
    