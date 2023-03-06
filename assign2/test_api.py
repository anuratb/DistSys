from lib.lib import *
import os
import threading
import random,time

def run_test(url="http://127.0.0.1:5124"):
    # Q = MyQueue(url)
    # t = Q.createTopic("News0")
    # print(t)
    res = requests.get(
        url + "/hello",
        params= {
            'A': '0'
        }
    )

if(__name__=='__main__'):
    run_test()
    