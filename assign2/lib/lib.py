import requests,random,time
import asyncio
import aiohttp

class MyQueue:
    
    def __init__(self,url:str):
        self.url = url
        self.loop = asyncio.get_event_loop()
    '''
    Builder function to create topic
    @param topicName 
    @return Topic object
    '''

    async def createTopic_(self, topicName:str):
        try:
            
            print(str("^^^^^^") + topicName)
            async with aiohttp.ClientSession() as session:
                async with session.post(self.url + "/topics",json={
                    "name": topicName
                }) as response:
                    res = await response.json()
            await session.close()
            print(res)
            if(res.get("status")=="Success"):
                print("Sucess")
                return self.Topic(self,topicName)
            else:
                #print(str(res.json()))
                raise Exception(res.get("message"))

        except Exception as err:
            return str(err)
    def createTopic(self, topicName:str):
        try:
            
            loop = asyncio.get_event_loop()
            obj = loop.run_until_complete(self.createTopic_(topicName))
            #loop.close()
            return obj

        except Exception as err:
            return str(err)

    async def get_partition_count_(self, topicName):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self.url + "/get_partition_count",
                    params = {
                        'topic': topicName
                    }
                ) as response:
                    res = await response.json()
            await session.close()
            if(res.get("status")=="Success"):
                return res.get("count")
            else:
                raise Exception(res.get("message"))
        except Exception as err:
            raise err
    
    def get_partition_count(self, topicName):
        try:
            loop = asyncio.get_event_loop()
            obj =  loop.run_until_complete(self.get_partition_count_(topicName))
            #loop.close()
            return obj
        except Exception as err:
            raise err

    '''
    Method To get all Topics
    '''
    async def get_all_topics_(self):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.url + "/topics") as response:
                    res = await response.json()
            await session.close()

            if(res.get("status")=="Success"):
                return res.get("topics")
            else:
                raise Exception(res.get("message"))
        except Exception as err:
            return str(err)
    def get_all_topics(self):
        try:
            loop = asyncio.get_event_loop()
            obj = loop.run_until_complete(self.get_all_topics_())
            #loop.close()
            return obj
        except Exception as err:    
            raise err





    '''
    Builder function to create producer from topiclist
    @param topicNames
    @return Producer Object
    '''
    async def createClient_(self, topicMap, isProducer):
        try:
            ids = {}
            
            for topicName, partition in topicMap.items():
                regTopicName = topicName
                if partition:
                    regTopicName += '#' + str(partition)

                if regTopicName in ids: continue
                json = {
                    "topic": topicName,
                }
                if partition:
                    json["partition"] = partition

                url = self.url
                if isProducer:
                    url += "/producer/register"
                else:
                    url += "/consumer/register"
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        url,
                        json=json
                    ) as response:
                        res = await response.json()
                        await session.close()
                
                        if(res.get("status") != "Success"):
                            raise Exception(res.get("message"))
                        else:
                            pid = None
                            if isProducer:
                                pid = res.get("producer_id")
                            else:
                                pid = res.get("consumer_id")
                            ids[regTopicName] = pid
            if isProducer:
                return self.Producer(self, ids)
            else:
                return self.Consumer(self, ids)            

        except Exception as err:
            return str(err)
    def createClient(self, topicMap, isProducer):
        try:
            loop = asyncio.get_event_loop()
            obj = loop.run_until_complete(self.createClient_(topicMap, isProducer))
            #loop.close()
            return obj
        except Exception as err:
            raise err

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

        def __init__(self,outer, pids:dict, partition = None):
            #self.topicName = topicName
            self.pids = pids
            self.outer = outer
            self.partition = partition
        '''
        To add a new topic(already existing in db) to topicList of producer
        @param topicName
        
        '''
        async def registerTopic_(self, topicName: str):
            try:
                # Check if producer is already registered in topicName
                if topicName in self.pids: return
                json={
                        "topic": topicName
                }
                if(self.partition) :
                    json["partition"] = self.partition
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        self.outer.url+"/producer/register",
                        json=json
                    ) as response:
                        res = await response.json()
                await session.close()
                if(res.get("status") != "Success"):
                    raise Exception(res.get("message"))
                else:
                    pid = res.get("producer_id")
                    self.pids[topicName] = pid
            except Exception as err:
                return str(err)
        def registerTopic(self, topicName: str):
            try:
                loop = asyncio.get_event_loop()
                loop.run_until_complete(self.registerTopic_(topicName))
                #loop.close()
            except Exception as err:
                raise err

        '''
        To enqueue a message
        @param msg : Message
        @param topicName: topic Name
        returns 0 if success
        '''
        async def enqueue_(self, msg:str, topicName:str, partition = None):
            regTopicName = topicName
            if partition:
                regTopicName += '#' + str(partition)
            try:
                if regTopicName not in self.pids.keys():
                    raise Exception("Error: Topic {} not registered".format(topicName))
                id = self.pids[regTopicName]
                json = {
                    "topic": topicName,
                    "producer_id": str(id),
                    "message": msg,
                }

                if partition:
                    json["partition"] = str(partition)
                
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        self.outer.url+"/producer/produce",
                        json=json
                    ) as response:
                        res = await response.json()
                await session.close()
                if(res.get("status") == "Success"):
                    return 0
                else:
                    raise Exception(res.get("message"))
            except Exception as err:
                return str(err)
        def enqueue(self, msg:str, topicName:str, partition = None):
            try:
                loop = asyncio.get_event_loop()
                obj = loop.run_until_complete(self.enqueue_(msg, topicName, partition))
                #loop.close()
                return obj
            except Exception as err:
                raise err
    '''
    Consumer class
    '''
    class Consumer:

        def __init__(self,outer,cids:dict,partition = None):
            #self.topicName = topicName
            self.cids = cids
            self.outer = outer
            self.partition = partition

        def consume(self):
            while(True):
                try:
                    for topic in self.cids.keys():
                        delay = random.random()
                        time.sleep(delay)
                        msg = self.dequeue(topic)
                        if(msg):
                            
                            #TODO
                            print("Dequeued: {} by consumer {}".format(msg,self.cids[self.cids.keys()[0]]))
                    
                except:
                    continue

        # Used to register for a topic
        async def registerTopic_(self, topicName: str):
            try:
                # Check if producer is already registered in topicName
                if topicName in self.cids: return
                json={
                    "topic": topicName
                }
                if(self.partition) :
                    json["partition"] = self.partition
                
                async with aiohttp.ClientSession() as session:
                    async with requests.post(
                        self.outer.url+"/consumer/register",
                        json=json
                    ) as response:
                        res = await response.json()
                await session.close()
                if(res.get("status") != "Success"):
                    raise Exception(res.get("message"))
                else:
                    cid = res.get("consumer_id")
                    self.cids[topicName] = cid
                    
            except Exception as err:
                return str(err)
        def registerTopic(self, topicName: str):
            try:
                loop = asyncio.get_event_loop()
                loop.run_until_complete(self.registerTopic_(topicName))
                #loop.close()
            except Exception as err:
                raise err
        '''
        To dequeue from a topic subscribed by consumer
        @param topicName
        @return The message dequeued
        '''
        async def dequeue_(self, topicName:str, partition = None):
            regTopicName = topicName
            if partition:
                regTopicName += '#' + str(partition)
            try:
                if regTopicName not in self.cids.keys():
                    raise Exception("Error: Topic not registered")
                id = self.cids[regTopicName]
                params = {
                    "topic": topicName,
                    "consumer_id": str(id)
                }

                if partition:
                    params["partition"] = str(partition)
                with aiohttp.ClientSession() as session:
                    async with session.get(
                        self.outer.url + "/consumer/consume",
                        params = params
                    ) as response:
                        res = await response.json()
                await session.close()

                if(res.get("status") == "Success"):
                    return res.get("message")
                else:
                    raise Exception(res.get("message"))
            except Exception as err:
                return None
        def dequeue(self, topicName:str, partition = None):
            try:
                loop = asyncio.get_event_loop()
                obj = loop.run_until_complete(self.dequeue_(topicName, partition))
                #loop.close()
                return obj
            except Exception as err:
                raise err

        '''
        Method to get queue size belonging to some topic name
        @param topicName
        @return size of the queue
        '''
        async def getSize_(self,topicName,partition):
            if(topicName not in self.cids.keys()):
                raise Exception("Error: Topic not registered")
            try:
                with aiohttp.ClientSession() as session:
                    async with session.get(
                        self.outer.url+"/size",
                        params={
                            "topic":topicName,
                            "consumer_id":self.cids[topicName],
                            "partition":partition
                        }
                    ) as response:
                        res = await response.json()
                await session.close()
               
                if(res.get("status")=="Success"):
                    return int(res.get("size"))
                else:
                    raise Exception(str(res.json().get("message")))
                
            except Exception as err:
                return str(err)
        def getSize(self,topicName,partition):
            try:
                loop = asyncio.get_event_loop()
                obj = loop.run_until_complete(self.getSize_(topicName,partition))
                #loop.close()
                return obj
            except Exception as err:
                raise err


