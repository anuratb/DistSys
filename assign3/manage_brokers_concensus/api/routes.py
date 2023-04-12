
from flask import request, redirect
from api import app
from api.data_struct import  Manager
from api.models import TopicDB
import os
from api import db,random,DB_URI, APP_URL,IsWriteManager

import requests
from api import readManagerURL
from flask_executor import Executor
from api.data_struct import getManager, is_leader, get_status

# TODO If not leader redirect to leader
def get_leader_val():
    leader = get_status()['leader']
    from api import raft_ip,manager_ip
    for i in range(0, len(raft_ip)):
        if(raft_ip[i]==leader):
            return "http://"+manager_ip[i]
    return None




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
    if not is_leader():
        print(get_leader_val()+"/topics")
        return redirect( get_leader_val()+"/topics", 307)
    print('Create Topic')
    topic_name : str = request.get_json().get('name')
    try:
        getManager().addTopic(topic_name)
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
    if not is_leader():
        return redirect(get_leader_val()+"/topics")
    try : 
        topic_list = getManager().getTopicsList()
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
    if not is_leader():
        return redirect(get_leader_val()+"/consumer/register",307)
    topic = request.get_json().get("topic")
    partition = request.get_json().get("partition")
    try:
        if partition:

            conID = getManager().Updates(
                "getBrokerConID",
                sync=True
            )
            ID_LIST = getManager().getBrokerList(topic, int(partition))
            brokerID = getManager().getAliveBroker(ID_LIST)
            if brokerID:
                brokerUrl = getManager().getBrokerUrlFromID(brokerID)
                return requests.post(
                    brokerUrl + "/consumer/register", 
                    json = {
                        "topic": topic,
                        "partition": partition,
                        "ID": conID,
                        "ID_LIST": ID_LIST
                    }
                ).json()
            
            raise Exception("Service currently unavailable")

        else:
            # Subscribe to all partitions of a topic
            retID = getManager().registerClientForAllPartitions("/consumer/register", topic, 1)
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
    if not is_leader():
        return redirect( get_leader_val()+"/producer/register",307)
    topic = request.get_json().get("topic")
    partition = request.get_json().get("partition")
    
    try:
        if partition:
            ID_LIST = getManager().getBrokerList(topic, int(partition))

            prodID = getManager().Updates(
                "getBrokerProdID",
                sync=True
            )
            brokerID = getManager().getAliveBroker(ID_LIST)
            if brokerID:
                brokerUrl = getManager().getBrokerUrlFromID(brokerID)
                return requests.post(
                    brokerUrl + "/producer/register", 
                    json = {
                        "topic": topic,
                        "partition": partition,
                        "ID": prodID,
                        "ID_LIST": ID_LIST
                    }
                ).json()
            raise Exception("Service currently unavailable")

        else:
            # Subscribe to all partitions of a topic
            retID = getManager().registerClientForAllPartitions("/producer/register", topic, 0)
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
    if not is_leader():
        return redirect(get_leader_val()+"/producer/produce",307)
    topic: str = request.get_json().get('topic')
    producer_id: str = request.get_json().get('producer_id')
    message: str = request.get_json().get('message')

    ID_LIST = None
    localProdID = producer_id
    brokerUrl = None
    brokerID = None
    partition = request.get_json().get('partition')
    msgID = getManager().getMsgIDWrapper()

    try:
        if producer_id[0] == '$':
            ID_LIST, localProdID, partition = getManager().getRRIndex(producer_id, topic, 0)    
        else:
            ID_LIST = getManager().getBrokerList(topic, int(partition))
        
        brokerID = getManager().getAliveBroker(ID_LIST)
        if not brokerID:
            raise Exception("Service currently unavailable")
        brokerUrl = getManager().getBrokerUrlFromID(brokerID)
        res = requests.post(
            brokerUrl + "/producer/produce",
            json = {
                "topic": topic,
                "producer_id": localProdID,
                "message":message,
                "partition":partition,
                "ID_LIST": ID_LIST,
                "msgID": msgID
            }
        )

        if res.status_code != 200:
            # Recover the broker
            requests.get(APP_URL + "/crash_recovery", params = {'brokerID': str(brokerID)})
            return {
                "status": "Failure",
                "message": "Service currently unavailable"
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
    
    try:
        topic: str = request.args.get('topic')
        consumer_id: str = request.args.get('consumer_id')
        partition = request.args.get('partition')
        if is_leader():
            from api import self_index
            while(True):
                ind = int(random() * len(readManagerURL))
                if ind!=self_index:
                    target_url = "http://"+readManagerURL[ind] + "/consumer/consume"
                    break
            
            url_with_param="{}?topic={}&consumer_id={}&from_leader={}".format(target_url, topic,consumer_id,True)
            
            if consumer_id[0] != '$':
                partition = request.args.get('partition')
                url_with_param = f"{url_with_param}&partition={partition}"
            print(url_with_param)
            #return requests.get(url_with_param).json()
            return redirect(url_with_param) #redirect to read manager
        else:
            # Check if its from a leader
            from_leader = request.args.get('from_leader')
            if from_leader:
                if consumer_id[0] == '$':
                    ID_LIST, localConID, partition = getManager().getRRIndex(consumer_id, topic, 1)
                    brokerID = getManager().getAliveBroker(ID_LIST)
                    if not brokerID:
                        raise Exception("Service currently unavailable")
                    brokerUrl = getManager().getBrokerUrlFromID(brokerID)
                    res = requests.get(
                        brokerUrl + "/consumer/consume",
                        params = {
                            "topic": topic,
                            "consumer_id": str(localConID),
                            "partition":partition,
                            "ID_LIST": ID_LIST,
                        }
                    )
                    if res.status_code != 200:
                        # Recover the broker
                        requests.get(APP_URL + "/crash_recovery", params = {'brokerID': str(brokerID)})
                        return {
                            "status": "Failure",
                            "message": "Service currently unavailable"
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
                    ID_LIST = getManager().getBrokerList(topic, partition)
                    brokerID = getManager().getAliveBroker(ID_LIST)
                    if not brokerID:
                        raise Exception("Service currently unavailable")
                    brokerUrl = getManager().getBrokerUrlFromID(brokerID)
                    url_with_param = f"{brokerUrl}/consumer/consume?topic={topic}&consumer_id={consumer_id}&partition={partition}"
                    for id in ID_LIST:
                        url_with_param = f"{url_with_param}&ID_LIST={id}"
                    print(url_with_param)
                    #return requests.get(url_with_param).json()
                    return redirect(url_with_param)
            else:
                # Redirect to the leader
                val = get_status()['leader']
                from api import raft_ip,manager_ip
                for i,curval in enumerate(raft_ip):
                    if curval == val:
                        target_url = "http://"+ manager_ip[i] + "/consumer/consume"
                        break
                url_with_param="{}?topic={}&consumer_id={}&from_leader={}".format(target_url, topic,consumer_id, True)
            
                if consumer_id[0] != '$':
                    partition = request.args.get('partition')
                    url_with_param = f"{url_with_param}&partition={partition}"
                print(url_with_param)
                return redirect(url_with_param)

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
    if not is_leader():
        url_with_param = f"{get_leader_val()}/get_partition_count"
        url_with_param = f"{url_with_param}?topic={request.args.get('topic')}"
        return redirect(url_with_param)
    try:
        topic: str = request.args.get('topic')
        if topic not in getManager().Topics:
            raise Exception(f"Topic {topic} doesn't exist")
        return {
            "status": "Success",
            "count": str(getManager().Topics[topic][1])
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
        broker_obj = Manager.build_run("../../broker")
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
        executor.submit(getManager().restartBroker,  brokerID)
        return "success"
    except Exception as e:
        return str(e)

@app.route('/crash_recovery_manager')
def crashRecovaryManager():
    try:
        managerID = request.args.get('managerID')
        executor.submit(getManager().restartManager,  managerID)
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