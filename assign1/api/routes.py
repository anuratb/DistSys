
from flask import request, jsonify
from api import app, db
from api.data_struct import TopicNode,Queue

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
    topic_name : str = request.args.get('name' , type=str) 
    try : 
        Queue.createTopic(topic_name) 
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
        topic_list = Queue.listTopics()
        topic_string : str = ""

        for topic in topic_list.keys() :
            topic_string += topic + ", "
        
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
    topic = request.args.get("topic",type=str)
    try:
        cid = Queue.registerConsumer(topic)
        return {
            "status":"Sucess",
            "consumer_id":cid
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
    topic = request.args.get("topic",type=str)
    try:
        pid = Queue.registerProducer(topic)
        return {
            "status":"Sucess",
            "producer_id":pid
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
    Response:
    "status": <string>
    onSuccess:
    None
    onFailure:
    - "message": <string> // Error message
'''

@ app.route("/producer/produce", methods=['POST'])
def enqueue():
    topic: str = request.args.get('topic', type=str)
    producer_id: int = request.args.get('producer_id', type=int)
    message: str = request.args.get('message', type=str)

    try:
        Queue.enqueue(topic, producer_id , message)
        return {
            "status": "Success",
            "message": ""
        }

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
    topic: str = request.args.get('topic', type=str)
    consumer_id: int = request.args.get('consumer_id', type=int)

    try :
        Queue.dequeue(topic, consumer_id)
        return {
            "status": "Success",
            "message": f"Topic {topic} dequed for consumer {consumer_id}!!"
        }

    except Exception as e : 
        return {
            "status" : "Failure" ,
             "message" : str(e)
            }
    

'''
    Size

    Return the number of log messages in the requested topic for this consumer.
    Method: GET
    Endpoint: /size
    Params:
    - "topic": <string>
    - "consumer_id": <int>
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
        queue_size : int = Queue.getSize(topic , consumer_id) 
        return {
            "status" : "Success" , 
            "size" : queue_size 
            }
    
    except Exception as e: 
        return {
            "status" : "Failure" ,
             "message" : str(e)
            }


