from lib.lib import *
import os
import threading
import random,time
def run_test(url="http://127.0.0.1:5124"):
    Q = MyQueue(url)
    path = "./test_asgn1"
    fp = open(path+"/prod_cons.txt")
    topics = []
    prods = {}
    cons = {}
    for line in fp.readlines():
        line = line.split(':')
        for wrd in line:
            wrd = wrd.strip()
        topic = line[0]
        prods_ = line[1].split()
        cons_ = line[2].split()
        print(topic)
        while(True):
            try:
                Q.createTopic(topic)
                break
            except:
                continue
        topics.append(topic)
        for prod in prods_:
            if(prod not in prods.keys()): prods[prod] = []
            prods[prod].append(topic)
        for con in cons_:
            if(con not in cons.keys()): cons[con] = []
            cons[con].append(topic)
    pobj = {}
    conobj = {}
    print(prods.keys())
    while(True):
        try:
            for prod,topics in prods.items():
                pobj[prod] = Q.createProducer(topics)
            for con,topics in cons.items():
                conobj[con] = Q.createConsumer(topics)
            break
        except:
            continue
    
    prod_msg = {}
    def produce(prod):
        print("Running Pthread "+str(prod_msg[prod][:5]))
        i = 0
        while(i<len(prod_msg[prod])):
            try:
                msg,topic = prod_msg[prod][i]
                cur:MyQueue.Producer = pobj[prod]
                delay = random.random()
                time.sleep(delay)
                cur.enqueue(msg,topic)
                i+=1
            except:
                continue
        '''
        for msg,topic in prod_msg[prod]:
            cur:MyQueue.Producer = pobj[prod]
            delay = random.random()
            time.sleep(delay)
            cur.enqueue(msg,topic)
        '''
    def consume(con):
        cur:MyQueue.Consumer = conobj[con]
        while(True):
            try:
                for topic in cons[con]:
                    delay = random.random()
                    time.sleep(delay)
                    
                    if(cur.getSize(topic)>0):
                        msg = cur.dequeue(topic)
                        print("Dequeued: {} by consumer {}".format(msg,con))
                
            except:
                continue
             
    for file in os.listdir(path):
        nm,ext = file.split('.')
        tp,id = nm.split('_')
        if(tp=="producer" and ext== "txt"):
            prod_msg["P"+str(id)] = []
            for line in open(path+"/"+file).readlines():
                #print(line.split())
                tm,msg,_,topic = line.split()
                topic = "T"+topic[2:3]
                prod_msg["P"+str(id)].append((line,topic))
    print(prod_msg.keys())
    pt = []
    ct = []
    for prod in prods.keys():
        pt.append(threading.Thread(target=produce,args=(prod,)))
    for con in cons.keys():
        ct.append(threading.Thread(target=consume,args=(con,)))
    for t in pt:
        t.start()
    
    for c in ct:
        c.start()
    for t in pt:
        t.join()
    for c in ct:
        c.join()
if(__name__=='__main__'):
    run_test()
    