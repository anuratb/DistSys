from lib.lib_sync import *
import os
import multiprocessing, threading
import random, time, re

PATH = "./test_asgn1"

def producer(url, prod_name, topics):
    print(f"Producer {prod_name} started...")

    Q = MyQueue(url)
    P = None
    topicMap = {}

    for i in range(len(topics)):
        if topics[i][-1] == '#':
            topics[i] = topics[i][:-1]
            while True:
                try:
                    numPartitions = int(Q.get_partition_count(topics[i]))
                    
                    break
                except:
                    continue
            
            partition = int(random.random() * numPartitions) + 1
            topicMap[topics[i]] = partition
        else:
            topicMap[topics[i]] = None

    while True:
        try:
            P = Q.createClient(topicMap, True)
            print(f"{prod_name}: Producer successfully created.")
            break
        except Exception as e:
            print(f"{prod_name}: Producer creation Error: " + str(e))

    prod_msg = []
             
    for file in os.listdir(PATH):
        nm, ext = file.split('.')
        tp, id = nm.split('_')
        if id != prod_name[-1]: continue
        if(tp == "producer" and ext == "txt"):
            for line in open(PATH + "/" + file).readlines():
                _, msg, _, topic = line.split()
                topic = "T" + topic[2:3]
                prod_msg.append((topic, topicMap[topic], msg))

    for tup in prod_msg:
        topic, partition, msg = tup
        while True:
            time.sleep(random.random())
            try:
                ret = P.enqueue(msg, topic, partition)
                if ret == 0:
                    print(f"{prod_name}: SUCCESS: Enqueuing topic {topic}, partition {partition} with msg {msg}")
                else:
                    print(f"{prod_name}: FAILURE: Enqueuing topic {topic}, partition {partition} with msg {msg}: {ret}")
                    continue
                break
            except:
                continue
        

    
def consumer(url, con_name, topics):
    print(f"Consumer {con_name} started...")
    Q = MyQueue(url)
    C = None
    topicMap = {}

    for i in range(len(topics)):
        if topics[i][-1] == '#':
            topics[i] = topics[i][:-1]
            while(True):
                try:
                    numPartitions = int(Q.get_partition_count(topics[i]))
                    partition = int(random.random() * numPartitions) + 1
                    topicMap[topics[i]] = partition
                    break
                except:
                    continue
            
            
            
        else:
            topicMap[topics[i]] = 0

    while True:
        try:
            C = Q.createClient(topicMap, False)
            if type(C) == str:
                raise Exception(f"{con_name}: Consumer creation Error: " + C)
            print(f"{con_name}: Consumer successfully created.")
            break
        except Exception as e:
            print(f"{con_name}: Consumer creation Error: " + str(e))

    while True:
        for topic in topics:
            partition = topicMap[topic]
            while True:
                time.sleep(random.random())
                try:
                    ret = C.dequeue(topic, partition)
                    break
                except:
                    print(f"{con_name}: FAILURE: Dequeuing from topic {topic}, partition {partition}: Response: {ret}")
                    continue
            
            if ret:
                print(f"{con_name}: SUCCESS: Dequeuing from topic {topic}, partition {partition}: Response: {ret}")


def run_test(url = "http://127.0.0.1:5124"):

    fp = open(PATH + "/prod_cons.txt")
    topics = []
    prods = {}
    cons = {}

    prod_proc = []
    cons_proc = []
    Q = MyQueue(url)

    for line in fp.readlines():
        line = line.split(':')
        for wrd in line:
            wrd = wrd.strip()
        topic_ = line[0]
        prods_ = line[1].split()
        cons_ = line[2].split()
        while(True):
            try:
                print(topic_)
                Q.createTopic(topic_)
                break
            except:
                continue
        topics.append(topic_)
        
        for prod_ in prods_:
            prod = prod_
            topic = topic_
            if prod_[-1] == '#':
                prod = prod_[:-1]
                topic = topic + '#'
            if(prod not in prods.keys()): prods[prod] = []
            prods[prod].append(topic)

        for con_ in cons_:
            con = con_
            topic = topic_
            if con_[-1] == '#':
                con = con_[:-1]
                topic = topic_ + '#'
            if(con not in cons.keys()): cons[con] = []
            cons[con].append(topic)


    for prod, topics in prods.items():
        proc = multiprocessing.Process(target=producer, args=(url, prod, topics))
        prod_proc.append(proc)
        proc.start()

    for con, topics in cons.items():
        proc = multiprocessing.Process(target=consumer, args=(url, con, topics))
        cons_proc.append(proc)
        proc.start()

    for proc in prod_proc:
        proc.join()

    for con in cons_proc:
        con.join()

    return


if(__name__=='__main__'):
    run_test()
    