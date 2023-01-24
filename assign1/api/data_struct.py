
class TopicNode:
    
    def __init__(self, topicID_):
        self.topicID = topicID_
        self.producerList = [] # List of subscribed producers
        self.consumerList = [] # List of subscribed consumers

    def subscribeProducer(self, producerID_):
        self.producerList.append(producerID_)
    
    def subscribeConsumer(self, consumerID_):
        self.consumerList.append(consumerID_)


class Queue:
    # topic-wise message queues
    # Key: TopicID, Value: An array of messages
    queue = {}
    # Topic-wise locks
    # Key: TopicID, Value: lock
    locks = {}
    # Key: topicname, Value: TopicNode
    topics = {}
    # Key: Consumer ID, Value: offset in the topic queue
    consumers = {}

    @classmethod
    def createTopic(topicName):
        pass

    @classmethod
    def listTopics():
        pass

    @classmethod
    def registerConsumer(topicName):
        pass

    @classmethod
    def registerProducer(topicName):
        pass

    @classmethod
    def enqueue(topicName, prodID, msg):
        pass

    @classmethod
    def dequeue(topicName, conID):
        pass
    
    @classmethod
    def getSize(topicName, conID):
        pass