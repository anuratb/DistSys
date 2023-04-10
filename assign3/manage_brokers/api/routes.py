
from flask import request, redirect
from api import app
from api.data_struct import  Manager,Docker
from api.models import TopicDB
import os
from api import db,random,DB_URI, APP_URL

import requests
from api import IsWriteManager, readManagerURL
from flask_executor import Executor


executor = Executor(app)

'''
    a. CreateTopic

    This operation allows you to create a new topic in the queue. The topic would be
    added to a list of available topics, and producers and consumers would be able
    to subscribe to it. This operation takes a string parameter specifying the name of
    the topic to be created.

    Method: POST
    Endpoint: /topics
    Params:
    - "name": <string>
    Response:
    onSuccess:
    - "status": “success”
    - "message": <string>
    onFailure:
    - "status": “failure”
    - "message": <string> // informative error message

'''
@ app.route("/topics", methods=['POST'])
def create_topic():
    
    print('Create Topic')
    topic_name : str = request.get_json().get('name')
    try:
        Manager.topicMetaData.addTopic(topic_name)
        return {
            "status" : "Success" , 
            "message" : 'Topic {} created successfully'.format(topic_name)
        }
    
    except Exception as e: 
        return {
            "status" : "Failure" ,
            "message" : str(e)
        }

'''
    b. ListTopics

    This operation allows you to retrieve a list of all available topics in the distributed
    queue.
    Method: GET
    Endpoint: /topics
    Params:
    None
    Response:
    "status": <string>
    onSuccess:
    - "topics": List[<string>] // List of topic names
    onFailure:
    - "message": <string> // Error message
'''

@ app.route("/topics", methods=['GET'])
def list_topics():
    
    try : 
        topic_list = Manager.topicMetaData.getTopicsList()
        topic_string : str = ", ".join(topic_list)
        
        return {
            "status" : "Success" , 
            "topics" : topic_string 
        }
    
    except Exception as e: 
        return {
            "status" : "Failure" ,
            "message" : str(e)
        }

'''
    c. RegisterConsumer

    This operation allows a consumer to register for a specific topic with the queue. If
    the consumer is interested in more than one topic, they call this endpoint multiple
    times, each time getting a topic specific consumer_id. This endpoint should
    return an error if the requested topic doesn’t exist.
    Method: POST
    Endpoint: /consumer/register
    Params:
    "topic": <string>
    Response:
    "status": <string>
    onSuccess:
    - "consumer_id": <int>
    onFailure:
    - "message": <string> // Error message
'''

@ app.route("/consumer/register", methods=['POST'])
def register_consumer():
    topic = request.get_json().get("topic")
    partition = request.get_json().get("partition")
    try:
        if partition:
            ID_LIST = Manager.getBrokerList(topic, int(partition))
            brokerUrl = Manager.getBrokerUrlFromID(ID_LIST[int(random() * len(ID_LIST))])
            return requests.post(
                brokerUrl + "/consumer/register", 
                json = {
                    "topic": topic,
                    "partition": partition,
                    "ID_LIST": ID_LIST
                }
            ).json()
        else:
            # Subscribe to all partitions of a topic
            retID = Manager.registerClientForAllPartitions("/consumer/register", topic, False)
            return {
                "status": "Success",
                "consumer_id": str(retID)
            }

    except Exception as e:
        return {
            "status": "Failure",
            "message": str(e)
        }
    

'''
    d. RegisterProducer

    This operation allows a producer to register for a specific topic with the queue. A
    producer should register with exactly one topic. If the requested topic doesn’t
    exist, calling this endpoint should create the topic.
    Method: POST
    Endpoint: /producer/register
    Params:
    "topic": <string>
    Response:
    "status": <string>
    onSuccess:
    - "producer_id": <int>
    onFailure:
    - "message": <string> // Error message
'''

@ app.route("/producer/register", methods=['POST'])
def register_producer():
    topic = request.get_json().get("topic")
    partition = request.get_json().get("partition")
    
    try:
        if partition:
            ID_LIST = Manager.getBrokerList(topic, int(partition))
            brokerUrl = Manager.getBrokerUrlFromID(ID_LIST[int(random() * len(ID_LIST))])
            return requests.post(
                brokerUrl + "/producer/register", 
                json = {
                    "topic": topic,
                    "partition": partition,
                    "ID_LIST": ID_LIST
                }
            ).json()
        else:
            # Subscribe to all partitions of a topic
            retID = Manager.registerClientForAllPartitions("/producer/register", topic, True)
            return {
                "status": "Success",
                "producer_id": str(retID)
            }
            
    except Exception as e:
        return {
            "status": "Failure",
            "message": str(e)
        }


'''
    e. Enqueue

    Add a log message to the queue.
    Method: POST
    Endpoint: /producer/produce
    Params:
    - "topic": <string>
    - "producer_id": <int>
    - "message": <string> // Log message to enqueue
    - "partition_id": <int>[Opt]
    Response:
    "status": <string>
    onSuccess:
    None
    onFailure:
    - "message": <string> // Error message
'''

@ app.route("/producer/produce", methods=['POST'])
def enqueue():
    topic: str = request.get_json().get('topic')
    producer_id: str = request.get_json().get('producer_id')
    message: str = request.get_json().get('message')

    ID_LIST = None
    localProdID = None
    brokerUrl = None
    brokerID = None
    partition = None

    try:
        if producer_id[0] == '$':
            #Update topic rrindex
            #Manager.topicMetaData.Topics[topic][3].acquire()
            #rrIndex = Manager.topicMetaData.Topics[topic][2]
            #Manager.topicMetaData.Topics[topic][2] += 1
            #TODO sync
            ###################### DB Update ############################
            obj = TopicDB.query.filter_by(topicName = topic).first()
            rrIndex = obj.rrindex
            obj.rrindex = obj.rrindex + 1            
            db.session.commit()
            #############################################################
            #Manager.topicMetaData.Topics[topic][3].release()
            
            ID_LIST, localProdID, partition = Manager.producerMetaData.getRRIndex(producer_id, topic, rrIndex)    
        else:
            ID_LIST = Manager.getBrokerList(topic, int(partition))
        
        brokerID = ID_LIST[int(random() * len(ID_LIST))]
        brokerUrl = Manager.getBrokerUrlFromID(brokerID)
        res = requests.post(
            brokerUrl + "/producer/produce",
            json = {
                "topic": topic,
                "producer_id": localProdID,
                "message":message,
                "partition":partition,
                "ID_LIST": ID_LIST
            }
        )

        if res.status_code != 200:
            # Recover the broker
            requests.get(APP_URL + "/crash_recovery", params = {'brokerID': str(brokerID)})
            return {
                "status": "Failure",
                "message": "Service currently unavailable. Try again later."
            }
        elif(res.json().get("status") == "Success"):
            return {
                "status": "Success",
                "message": ""
            }
        else:
            raise Exception(res.json.get("message"))

    except Exception as e:
        return {
            "status": "Failure",
            "message": str(e)
        }
  

'''
    f. Dequeue

    Remove and return the next log message in the queue for the specified topic for
    this consumer.
    Method: GET
    Endpoint: /consumer/consume
    Params:
    - "topic": <string>
    - "consumer_id": <int>
    Response:
    "status": <string>
    onSuccess:
    - "message": <string> // Log message
    onFailure:
    - "message": <string> // Error message
'''



@ app.route("/consumer/consume", methods=['GET'])
def dequeue():
    topic: str = request.args.get('topic')
    consumer_id: str = request.args.get('consumer_id')
    partition = None
    try:
        if IsWriteManager:
            #Update topic rrindex
            #Manager.topicMetaData.Topics[topic][3].acquire()
            
            #Manager.topicMetaData.Topics[topic][2] += 1
            #Manager.topicMetaData.Topics[topic][3].release()
            #TODO sync
            ###################### DB Update ############################
            rrIndex = TopicDB.query.filter_by(topicName = topic).first().rrindex
            TopicDB.query.filter_by(topicName = topic).first().rrindex = rrIndex + 1
            db.session.commit()
            #############################################################
            
            
            ind = int(random() * len(readManagerURL))
            target_url = readManagerURL[ind] + "/consumer/consume"
            url_with_param="{}?topic={}&consumer_id={}&topicRRIndex={}".format(target_url, topic, consumer_id, rrIndex)
            
            if consumer_id[0] != '$':
                partition = request.args.get('partition')
                url_with_param = f"{url_with_param}&partition={partition}"

            return redirect(url_with_param, 307) #redirect to read manager
        else:
            if consumer_id[0] == '$':
                topicRRIndex = int(request.args.get('topicRRIndex'))
                brokerID, conID, partition = Manager.consumerMetaData.getRRIndex(consumer_id, topic, topicRRIndex)
                brokerUrl = Manager.getBrokerUrlFromID(brokerID)
                res = requests.get(
                    brokerUrl + "/consumer/consume",
                    params = {
                        "topic": topic,
                        "consumer_id": str(conID),
                        "partition":partition
                    }
                )
                if res.status_code != 200:
                    # Recover the broker
                    requests.get(APP_URL + "/crash_recovery", params = {'brokerID': str(brokerID)})
                    return {
                        "status": "Failure",
                        "message": "Service currently unavailable. Try again later."
                    }
                elif(res.json().get("status") == "Success"):
                    msg = res.json().get("message")
                    return {
                        "status": "Success",
                        "message": msg
                    }
                else:
                    raise Exception(res.json.get("message"))
            else:
                partition = request.args.get('partition')
                brokerUrl = Manager.getBrokerUrl(topic, int(partition))
                url_with_param = f"{brokerUrl}/consumer/consume?topic={topic}&consumer_id={consumer_id}&partition={partition}"
                return redirect(url_with_param, 307)

    except Exception as e:
        return {
            "status": "Failure",
            "message": str(e)
        }

'''
    Size

    Return the number of log messages in the requested topic for this consumer.
    Method: GET
    Endpoint: /size
    Params:
    - "topic": <string>
    - "consumer_id": <int>
    - "partition_id": <int> 
    Response:
    "status": <string>
    onSuccess:
    - "size": <int>
    onFailure:
    - "message": <string> // Error message
'''

@ app.route("/get_partition_count", methods=['GET'])
def getPartitionCount():
    try:
        topic: str = request.args.get('topic')
        if topic not in Manager.topicMetaData.Topics:
            raise Exception(f"Topic {topic} doesn't exist")
        return {
            "status": "Success",
            "count": str(Manager.topicMetaData.Topics[topic][1])
        }
    except Exception as e:
        return {
            "status": "Failure",
            "message": str(e)
        }

@ app.route("/size", methods=['GET'])
def size():
    topic : str = request.args.get('topic' , type=str) 
    consumer_id : int = request.args.get('consumer_id', type=int)
    
    try : 
        partition = request.get_json().get('partition')
        brokerUrl = Manager.getBrokerUrl(topic, int(partition))
        return requests.get(brokerUrl + "/size"
            , params = {
                "topic": topic,
                "consumer_id": str(consumer_id),
                "partition":str(partition)
            })
        #return redirect(brokerUrl + "/size", 307)
    
    except Exception as e: 
        return {
            "status" : "Failure" ,
             "message" : str(e)
            }


@app.route("/addbroker", methods=["POST","GET"])
def addBroker():
    try:
        broker_obj = Docker.build_run("../../broker")
        #Manager.lock.acquire()
        Manager.brokers[broker_obj.brokerID] = broker_obj
        #Manager.lock.release()
        return {
            "status" : "Success" ,
            "id": str(broker_obj.brokerID)
        }
    except Exception as e:
        return {
            "status" : "Failure" ,
            "message" : str(e)
        }
    


@app.route("/removebroker", methods=["POST"])
def removeBroker():
    #TODO assgn3
    #return NotImplementedError()
    try:
        brokerID = int(request.get_json().get('brokerID'))
        del Manager.brokers[brokerID]
        os.system(f"docker rm -f broker{brokerID}")
        return {
            "status" : "Success" 
        }
    except Exception as e:
        return {
            "status" : "Failure" ,
            "message" : str(e)
        }


@app.route('/crash_recovery')
def crashRecovary():
    try:
        brokerID = int(request.args.get('brokerID'))
        executor.submit(Docker.restartBroker,  brokerID)
        return "success"
    except Exception as e:
        return str(e)

@app.route('/crash_recovery_manager')
def crashRecovaryManager():
    try:
        managerID = request.args.get('managerID')
        executor.submit(Docker.restartManager,  managerID)
        return "success"
    except Exception as e:
        return str(e)


@app.route('/job')
def job():
    print("A")
    while True: pass
    # return str(100)

@app.route('/hello', methods=["GET"])
def hello():
    if IsWriteManager:
        print("HELL1")
        brokerID = int(request.args.get('brokerID'))
        ind = int(random()*len(readManagerURL))
        return redirect(readManagerURL[ind] + "/hello", 307) #redirect to read manager
    else:
        print("HELL2")
        brokerID = int(request.args.get('brokerID'))
        return {'brokerID': "$$$" + str(brokerID)}

@app.route('/isAlive', methods=["GET"])
def isAlive():
    return {
        "status": "Success"
    }