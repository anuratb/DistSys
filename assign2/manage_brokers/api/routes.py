
from flask import request, redirect
from api import app
from api.data_struct import brokersDocker, Manager
import requests


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
            brokerUrl = Manager.getBrokerUrl(topic, int(partition))
            return redirect(brokerUrl + "/consumer/register", 307)
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
            brokerUrl = Manager.getBrokerUrl(topic, int(partition))
            return redirect(brokerUrl + "/producer/register", 307)
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
    try:
        topic: str = request.get_json().get('topic')
        producer_id: str = request.get_json().get('producer_id')
        message: str = request.get_json().get('message')
        partition = request.get_json().get('partition')
        if producer_id[0] == '$':
            brokerID, prodID = Manager.producerMetaData.getRRIndex(producer_id, topic)
            brokerUrl = Manager.getBrokerUrlFromID(brokerID)

            res = requests.get(
                brokerUrl + "/producer/produce",
                params = {
                    "topic": topic,
                    "producer_id": prodID,
                    "message":message
                }
            )
            if(res.json().get("status") == "Success"):
                return {
                    "status": "Success",
                    "message": ""
                }
            else:
                raise Exception(res.json.get("message"))
        else:
            partition = request.get_json().get('partition')
            brokerUrl = Manager.getBrokerUrl(topic, int(partition))
            return redirect(brokerUrl + "/producer/produce", 307)

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
    try:
        topic: str = request.get_json().get('topic')
        consumer_id: str = request.get_json().get('consumer_id')

        if consumer_id[0] == '$':
            brokerID, conID = Manager.consumerMetaData.getRRIndex(consumer_id, topic)
            brokerUrl = Manager.getBrokerUrlFromID(brokerID)
            res = requests.get(
                brokerUrl + "/consumer/consume",
                params = {
                    "topic": topic,
                    "consumer_id": str(conID)
                }
            )
            if(res.json().get("status") == "Success"):
                msg = res.json().get("message")
                return {
                    "status": "Success",
                    "message": msg
                }
            else:
                raise Exception(res.json.get("message"))
        else:
            partition = request.get_json().get('partition')
            brokerUrl = Manager.getBrokerUrl(topic, int(partition))
            return redirect(brokerUrl + "/consumer/consumer", 307)

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

@ app.route("/size", methods=['GET'])
def size():
    topic : str = request.args.get('topic' , type=str) 
    consumer_id : int = request.args.get('consumer_id', type=int)
    
    try : 
        partition = request.get_json().get('partition')
        brokerUrl = Manager.getBrokerUrl(topic, int(partition))
        return redirect(brokerUrl + "/size", 307)
    
    except Exception as e: 
        return {
            "status" : "Failure" ,
             "message" : str(e)
            }


@app.route("/addbroker", methods=["POST"])
def addBroker():
    brokersDocker.build_run('../../broker/')


@app.route("/removebroker", methods=["POST"])
def removeBroker():
    return NotImplementedError()

