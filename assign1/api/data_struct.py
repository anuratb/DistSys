import threading
from api import db
from api.models import QueueDB,Topics,Producer,Consumer
class TopicNode:
    
    def __init__(self, topicID_):
        self.topicID = topicID_
        self.producerList = [0, 1] # List of subscribed producers
        self.plock = {
            0: threading.Lock()
        }
        self.consumerList = [0, 1] # List of subscribed consumers
        self.clock = {
            0: threading.Lock()
        }
        
    def subscribeProducer(self, producerID_):
        self.producerList.append(producerID_)
    
    def subscribeConsumer(self, consumerID_):
        self.consumerList.append(consumerID_)


class Queue:
    # topic-wise message queues
    # Key: TopicID, Value: An array of messages
    glob_lck = threading.Lock()
    queue = {
    }
    # Topic-wise locks
    # Key: TopicID, Value: lock
    locks = {
       # 0: threading.Lock()
    }
    # Key: topicname, Value: TopicNode
    topics = {
        #'A': TopicNode(0)
    }
    # Key: Consumer ID, Value: {offset in the topic queue, lock}
    consumers = {
       # 0: [0, threading.Lock()],
       # 1: [0, threading.Lock()]
    }
    cntCons = 0
    cntProd = 0
    cntMessage = 0
    @classmethod
    def clear(cls):
        cls.glob_lck = threading.Lock()
        cls.queue = {}
        # Topic-wise locks
        # Key: TopicID, Value: lock
        cls.locks = {}
        # Key: topicname, Value: TopicNode
        cls.topics = {}
        # Key: Consumer ID, Value: {offset in the topic queue, lock}
        cls.consumers = {}
        cls.cntCons=0
        cls.cntMessage=0
        cls.cntProd = 0
    @classmethod
    def createTopic(cls, topicName):
        if topicName in cls.topics.keys():
            raise Exception('Topicname: {} already exists'.format(topicName))
        cls.glob_lck.acquire()
        nid  = len(cls.topics)
        cls.topics[topicName] = TopicNode(nid)
        cls.queue[nid] = []
        cls.locks[nid] = threading.Lock()
        #db updates
        db.session.add(Topics(id=nid,value=topicName))
        db.session.commit()
        cls.glob_lck.release()

    @classmethod
    def listTopics(cls):
        return cls.topics

    @classmethod
    def registerConsumer(cls, topicName):
        if not topicName in cls.topics.keys():
            raise Exception('Topicname: {} does not exists'.format(topicName))
        topicID = cls.topics[topicName].topicID
        lock = cls.locks[topicID]
        lock.acquire()
        nid = cls.cntCons
        cls.cntCons+=1
        cls.topics[topicName].consumerList.append(nid)
        cls.consumers[nid]=[nid,threading.Lock()]
        #db updates
        obj = Consumer(id=nid,offset=0)
        cur = Topics.query.filter_by(id=topicID).first()
        cur.consumers.append(obj)
        db.session.add(obj)
        db.session.commit()
        lock.release()
        return nid
            
    @classmethod
    def registerProducer(cls, topicName):
        if topicName not in cls.topics.keys():
            cls.createTopic(topicName)
        topicID = cls.topics[topicName].topicID
        lock = cls.locks[topicID]
        lock.acquire()
        nid = cls.cntProd
        cls.cntProd+=1
        cls.topics[topicName].producerList.append(nid)
        #db updates
        print(topicID)
        obj = Producer(id=nid)
        cur = Topics.query.filter_by(id=topicID).first()
        cur.producers.append(obj)
        db.session.add(obj)
        db.session.commit()
        lock.release()
        return nid

    @classmethod
    def enqueue(cls, topicName, prodID, msg):
        # Check if topic exists
        try:
            topicID = cls.topics[topicName].topicID
        except Exception as _:
            raise Exception("Error: No such topic exists!")
        
        # Check if user is registered for the topic
        if prodID not in cls.topics.get(topicName).producerList:
            raise Exception("Error: Invalid producer ID!")

        # Get the lock for the queue with topicName
        lock = cls.locks[topicID]
        cls.glob_lck.acquire()
        nid = cls.cntMessage
        cls.cntMessage+=1
        cls.glob_lck.release()
        prev_id = None
        lock.acquire()
        if(len(cls.queue[topicID])>0): prev_id = cls.queue[topicID][-1][0]
        cls.queue[topicID].append([nid,msg])
        #DB updates
        if(prev_id is None):
            obj = QueueDB(id = nid,value=msg)
            db.session.add(obj)
            topic = Topics.query.filter_by(id=topicID).first()
            topic.start_ind = nid
            topic.end_ind = nid
            db.session.commit()
            
        else:
            obj = QueueDB(id = nid,value=msg)
            db.session.add(obj)
            topic = Topics.query.filter_by(id=topicID).first()
            prevMsg = QueueDB.query.filter_by(id=prev_id).first()
            prevMsg.nxt_id = nid
            topic.end_ind = nid
            db.session.commit()
        lock.release()


    @classmethod
    def dequeue(cls, topicName, conID):
        # Check if topic exists
        try:
            topicID = cls.topics[topicName].topicID
        except Exception as _:
            raise Exception("Error: No such topic exists!")
        
        # Check if user is registered for the topic
        if conID not in cls.topics.get(topicName).consumerList:
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
            return msg
        else:
            lock.release()
            raise Exception("There are no new messages!")
        

    @classmethod
    def getSize(cls, topicName, conID):
        if(topicName not in cls.topics.keys()):
            raise Exception("Error: No such topic, {} exists!".format(topicName))
        # Check if user is registered for the topic
        if conID not in cls.topics.get(topicName).consumerList:
            raise Exception("Error: Invalid consumer ID!")
        return len(cls.queue[cls.topics[topicName].topicID])-cls.consumers.get(conID)[0]
        