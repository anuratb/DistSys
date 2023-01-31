import requests

class MyQueue:
    
    def __init__(self,url:str):
        self.url = url
    '''
    Builder function to create topic
    @param topicName 
    @return Topic object
    '''

    def createTopic(self,topicName:str):
        try:
            #print(str("^^^^^^")+topicName)
            res = requests.post(self.url+"/topics",json={
                "name":topicName
            })
            if(res.json().get("status")=="Success"):
                return self.Topic(self,topicName)
            else:
                #print(str(res.json()))
                raise Exception(res.json().get("message"))

        except Exception as err:
            return str(err)

    '''
    Method To get all Topics
    '''
    def get_all_topics(self):
        try:
            res = requests.get(self.url+"/topics")
            if(res.json().get("status")=="Success"):
                return res.json().get("topics")
            else:
                raise Exception(res.json().get("message"))
        except Exception as err:
            return str(err)

    '''
    Builder function to create producer from topiclist
    @param topicNames
    @return Producer Object
    '''
    def createProducer(self,topicNames:list):
        try:
            ids = {}
            for topicName in topicNames:
                # Check if producer is already registered in topicName
                if topicName in ids: continue
                res = requests.post(
                    self.url+"/producer/register",
                    json={
                        "topic":topicName
                    })
                if(res.json().get("status")!="Success"):
                    raise Exception(res.json().get("message"))
                else:
                    pid = res.json().get("producer_id")
                    ids[topicName] = pid
            return self.Producer(self,ids)                

        except Exception as err:
            return str(err)
    '''
    Builder function to create consumer
    @param topicNames
    @return Consumer Object
    '''
    def createConsumer(self,topicNames:list):
        try:
            ids = {}
            for topicName in topicNames:
                # Check if consumer is already registered in topicName
                if topicName in ids: continue
                res = requests.post(
                    self.url+"/consumer/register",
                    json={
                        "topic":topicName
                    })
                if(res.json().get("status")!="Success"):
                    raise Exception(res.json().get("message"))
                else:
                    cid = res.json().get("consumer_id")
                    ids[topicName] = cid
            return self.Consumer(self,ids)                

        except Exception as err:
            raise str(err)
    '''
    Topic class
    '''
    class Topic:
        def __init__(self,outer,topicName:str):
            self.topicName = topicName
            self.outer = outer
    '''
    Producer Class
    '''
    class Producer:

        def __init__(self,outer, pids:dict):
            #self.topicName = topicName
            self.pids = pids
            self.outer = outer
        '''
        To add a new topic(already existing in db) to topicList of producer
        @param topicName
        
        '''
        def registerTopic(self, topicName: str):
            try:
                # Check if producer is already registered in topicName
                if topicName in self.pids: return
                res = requests.post(
                    self.url+"/producer/register",
                    json={
                        "topic": topicName
                    }
                )
                if(res.json().get("status") != "Success"):
                    raise Exception(res.json().get("message"))
                else:
                    pid = res.json().get("producer_id")
                    self.pids[topicName] = pid
            except Exception as err:
                return str(err)
        '''
        To enqueue a message
        @param msg : Message
        @param topicName: topic Name
        returns 0 if success
        '''
        def enqueue(self,msg:str,topicName:str):
            if(topicName not in self.pids.keys()):
                raise Exception("Error: Topic {} not registered".format(topicName))
            try:
                id = self.pids[topicName]
                res = requests.post(
                    self.outer.url+"/producer/produce",
                    json={
                        "topic":topicName,
                        "producer_id":id,
                        "message":msg
                    }
                )
                if(res.json().get("status")=="Success"):
                    return 0
                else:
                    raise Exception(res.json.get("message"))
            except Exception as err:
                return str(err)
    '''
    Consumer class
    '''
    class Consumer:

        def __init__(self,outer,cids:dict):
            #self.topicName = topicName
            self.cids = cids
            self.outer = outer

        # Used to register for a topic
        def registerTopic(self, topicName: str):
            try:
                # Check if producer is already registered in topicName
                if topicName in self.cids: return
                res = requests.post(
                    self.url+"/consumer/register",
                    json={
                        "topic": topicName
                    }
                )
                if(res.json().get("status") != "Success"):
                    raise Exception(res.json().get("message"))
                else:
                    cid = res.json().get("consumer_id")
                    self.cids[topicName] = cid
            except Exception as err:
                return str(err)
        '''
        To dequeue from a topic subscribed by consumer
        @param topicName
        @return The message dequeued
        '''
        def dequeue(self,topicName:str):
            if(topicName not in self.cids.keys()):
                raise Exception("Error: Topic not registered")
            try:
                id = self.cids[topicName]
                res = requests.get(
                    self.outer.url+"/consumer/consume",
                    params={
                        "topic":topicName,
                        "consumer_id":id
                    }
                )
                if(res.json().get("status")=="Success"):
                    return res.json().get("message")
                else:
                    raise Exception(res.json.get("message"))
            except Exception as err:
                return str(err)
        '''
        Method to get queue size belonging to some topic name
        @param topicName
        @return size of the queue
        '''
        def getSize(self,topicName):
            if(topicName not in self.cids.keys()):
                raise Exception("Error: Topic not registered")
            try:
                res = requests.get(
                    self.outer.url+"/size",
                    params={
                        "topic":topicName,
                        "consumer_id":self.cids[topicName]
                    }
                )
                if(res.json().get("status")=="Success"):
                    return int(res.json().get("size"))
                else:
                    raise Exception(str(res.json().get("message")))
                
            except Exception as err:
                return str(err)


