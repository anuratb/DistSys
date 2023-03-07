from lib.lib import *
import os
import multiprocessing, threading
import random, time, re

PATH = "./test_asgn2"

def producer(url, prod_name, topics):
    print(f"Producer {prod_name} started...")

    Q = MyQueue(url)
    P = None
    topicMap = {}

    for i in range(len(topics)):
        if topics[i][-1] == '#':
            topics[i] = topics[i][:-1]
            numPartitions = Q.get_partition_count(topics[i])
            partition = int(random.random() * numPartitions) + 1
            topicMap[topics[i]] = partition
        else:
            topicMap[topics[i]] = 0

    while True:
        try:
            P = Q.createProducer(topicMap, True)
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
        ret = P.enqueue(msg, topic, partition)
        if ret == 0:
            print(f"{prod_name}: Enqueuing topic {topic}, partition {partition} with msg {msg}: SUCCESS")
        else:
            print(f"{prod_name}: Enqueuing topic {topic}, partition {partition} with msg {msg}: FAILURE: {ret}")

    
def consumer(url, con_name, topics):
    print(f"Consumer {con_name} started...")
    Q = MyQueue(url)
    C = None
    topicMap = {}

    for i in range(len(topics)):
        if topics[i][-1] == '#':
            topics[i] = topics[i][:-1]
            numPartitions = Q.get_partition_count(topics[i])
            partition = int(random.random() * numPartitions) + 1
            topicMap[topics[i]] = partition
        else:
            topicMap[topics[i]] = 0

    while True:
        try:
            C = Q.createConsumer(topicMap, False)
            print(f"{con_name}: Consumer successfully created.")
            break
        except Exception as e:
            print(f"{con_name}: Consumer creation Error: " + str(e))

    while True:
        for topic in topics:
            partition = topicMap[topic]
            ret = C.dequeue(topic, partition)
            print(f"{con_name}: Dequeuing from topic {topic}, partition {partition}: Response: {ret}")


def run_test(url = "http://127.0.0.1:5124"):

    fp = open(PATH + "/prod_cons.txt")
    topics = []
    prods = {}
    cons = {}

    prod_proc = []
    cons_proc = []

    for line in fp.readlines():
        line = line.split(':')
        for wrd in line:
            wrd = wrd.strip()
        topic_ = line[0]
        prods_ = line[1].split()
        cons_ = line[2].split()

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
    