import threading

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
        0: []
    }
    # Topic-wise locks
    # Key: TopicID, Value: lock
    locks = {
        0: threading.Lock()
    }
    # Key: topicname, Value: TopicNode
    topics = {
        'A': TopicNode(0)
    }
    # Key: Consumer ID, Value: {offset in the topic queue, lock}
    consumers = {
        0: [0, threading.Lock()],
        1: [0, threading.Lock()]
    }
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
    @classmethod
    def createTopic(cls, topicName):
        if topicName in cls.topics.keys():
            raise Exception('Topicname: {} already exists'.format(topicName))
        cls.glob_lck.acquire()
        nid  = len(cls.topics)
        cls.topics[topicName] = TopicNode(nid)
        cls.queue[nid] = []
        cls.locks[nid] = threading.Lock()
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
        nid = len(cls.topics[topicName].consumerList)
        cls.topics[topicName].consumerList.append(nid)
        cls.consumers[nid]=[nid,threading.Lock()]
        lock.release()
        return nid
            
    @classmethod
    def registerProducer(cls, topicName):
        if topicName not in cls.topics.keys():
            cls.createTopic(topicName)
        topicID = cls.topics[topicName].topicID
        lock = cls.locks[topicID]
        lock.acquire()
        nid = len(cls.topics[topicName].producerList)
        cls.topics[topicName].producerList.append(nid)
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
        lock.acquire()
        cls.queue[topicID].append(msg)
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
        