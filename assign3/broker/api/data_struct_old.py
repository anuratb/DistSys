import threading
from api import db
from api.models import QueueDB,Topics,Producer,Consumer
from pysyncobj import SyncObj, replicated

class TopicNode(SyncObj):
    
    def __init__(self, selfAddr, otherAddrs):
        super().__init__(selfAddr, otherAddrs)
        self.topicID = None
        self.topicName = None
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

class TopicList:
    # topicName -> topicNode
    Tlist = {}
    # topicName -> Lock
    NodeLock = {}
    Tlock = threading.Lock()

    @classmethod
    def createTopicNode(cls, selfAddr, otherAddrs):
        node = TopicNode(selfAddr, otherAddrs)
        while node.getTopicName() is None:
            print("createTopicNode: Error")
        cls.Tlist[node.getTopicName()] = node
        cls.NodeLock[node.getTopicName()] = threading.Lock()

    @classmethod
    def addTopic(cls, selfAddr, otherAddrs, topicName, topicID = None):
        cls.Tlock.acquire()
        if not topicID:
            topicID = len(cls.Tlist)
        cls.Tlist[topicName] = TopicNode(selfAddr, otherAddrs)
        cls.NodeLock[topicName] = threading.Lock()
        cls.Tlock.release()

        cls.Tlist[topicName].setTopic(topicID, topicName, sync = True)
    
        return topicID

    @classmethod
    def listTopics(cls):
        return [topic.getTopicName() for (_, topic) in cls.Tlist.items()]

    @classmethod
    def isValidTopic(cls, topicName):
        if topicName in cls.Tlist:
            return True
        else:
            return False

    @classmethod
    def addConsumer(cls, topicName, conID):
        cls.NodeLock[topicName].acquire()
        cls.Tlist[topicName].subscribeConsumer(conID, sync = True)
        cls.NodeLock[topicName].release()


    @classmethod
    def addProducer(cls, topicName, prodID):
        cls.NodeLock[topicName].acquire()
        cls.Tlist[topicName].subscribeProducer(prodID, sync = True)
        cls.NodeLock[topicName].release()


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
    def addOffset(self, conID):
        self.Offset[conID] = [0, threading.Lock()]

    @replicated
    def getUpdOffset(self, conID):
        offset = self.Offset[conID][0]
        self.Offset[conID] = [0, threading.Lock()]

    @replicated
    def addMessage(self, nid, topicID, msg):
        prev_id = None
        if len(self.queue):
            prev_id = self.queue[-1][0]
        self.queue.append([nid, msg])
        
        # TODO: check if the data is already in database
        obj = QueueDB(id = nid,value = msg)
        db.session.add(obj)
        topic = Topics.query.filter_by(id = topicID).first()
        if prev_id is None:
            topic.start_ind = nid  
        else:
            prevMsg = QueueDB.query.filter_by(id = prev_id).first()
            prevMsg.nxt_id = nid
        topic.end_ind = nid
        db.session.commit()

    def getMessage(self, conID):
        pass

    def getRemainingSize(self, conID):
        return len(self.queue) - self.Offset[conID][0]

class QueueList:
    nxtConID = 0
    cLock = threading.Lock()
    nxtProdID = 0
    pLock = threading.Lock()
    msgID = 0
    msgLock = threading.Lock()
    # topicName -> Queue
    QList = {}
    # topicName -> Lock
    QLock = {}
    # ConsumerID -> Lock
    offsetLock = {}

    @classmethod
    def addQueue(cls, selfAddr, otherAddrs, topicName):
        cls.QList[topicName] = Queue(selfAddr, otherAddrs)
        cls.QLock[topicName] = threading.Lock()

    @classmethod
    def registerConsumer(cls, topicName):
        if not TopicList.isValidTopic(topicName):
            raise Exception('Topicname: {} does not exists'.format(topicName))

        cls.cLock.acquire()
        nid = cls.nxtConID
        cls.nxtConID += 1
        cls.cLock.release()

        TopicList.addConsumer(topicName, nid)
        cls.QList[topicName].addOffset(nid, sync = True)

        return nid

    @classmethod
    def registerProducer(cls, topicName):
        if not TopicList.isValidTopic(topicName):
            raise Exception('Topicname: {} does not exists'.format(topicName))
        
        cls.pLock.acquire()
        nid = cls.nxtProdID
        cls.nxtProdID += 1
        cls.pLock.release()

        TopicList.addProducer(topicName, nid)

        return nid

    @classmethod
    def enqueue(cls, topicName, prodID, msg):
        
        #print("Enqueueing: ", topicName, prodID, msg)
        #print(cls.topics.get(topicName).producerList)
      
        # Check if topic exists
        if not TopicList.isValidTopic(topicName):
            raise Exception('Topicname: {} does not exists'.format(topicName))

        #print("----------------------")
        # Check if user is registered for the topic
        topicNode = TopicList.Tlist[topicName]
        if not topicNode.isProdRegistered(prodID):
            raise Exception("Error: Invalid producer ID!")

        cls.msgLock.acquire()
        nid = cls.msgID
        cls.msgID += 1
        cls.msgLock.release()

        cls.QLock[topicName].acquire()
        cls.QList[topicName].addMessage(nid, topicNode.getTopicID(), msg, sync = True)
        cls.QLock[topicName].release()

    @classmethod
    @replicated
    def dequeue(cls, topicName, conID):
        # Check if topic exists
        if not TopicList.isValidTopic(topicName):
            raise Exception('Topicname: {} does not exists'.format(topicName))
        
        # Check if user is registered for the topic
        topicNode = TopicList.Tlist[topicName]
        if not topicNode.isConRegistered(conID):
            raise Exception("Error: Invalid consumer ID!")
        
        # Get the offset and lock
        lock = cls.consumers.get(conID)[1]
        lock.acquire()
        Q = cls.queue[topicID]
        index = cls.consumers.get(conID)[0]
        l = len(Q)
        # Check if messages are yet to be read
        if index < l:
            msg = Q[index]
            # Update the offset/index
            cls.consumers.get(conID)[0] += 1
            obj = Consumer.query.filter_by(id=conID).first()
            obj.offset+=1
            db.session.commit()
            lock.release()
            return msg[1]
        else:
            lock.release()
            raise Exception("There are no new messages!")

    @classmethod
    def getSize(cls, topicName, conID):
        if not TopicList.isValidTopic(topicName):
            raise Exception('Topicname: {} does not exists'.format(topicName))
        # Check if user is registered for the topic
        topicNode = TopicList.Tlist[topicName]
        if not topicNode.isConRegistered(conID):
            raise Exception("Error: Invalid consumer ID!")

        return cls.QList[topicName].getRemainingSize(conID)
        
        