#!/usr/bin/env python
from __future__ import print_function

import sys
import time
from functools import partial
sys.path.append("../")
from pysyncobj import SyncObj, replicated
import threading
import multiprocessing
from pysyncobj.batteries import ReplLockManager


class User(SyncObj):

    def __init__(self, selfNodeAddr, otherNodeAddrs):
        super(User, self).__init__(selfNodeAddr, otherNodeAddrs)
        # UserName -> List of Accounts
        self.counter = 0
        self.lock = threading.Lock()
        
    @replicated
    def incCounter(self):
        old = self.counter
        self.counter += 1
        return old

    
    def getCounter(self):
        return self.counter

    
class Account(SyncObj):

    def __init__(self, selfNodeAddr, otherNodeAddrs):
        super(Account, self).__init__(selfNodeAddr, otherNodeAddrs)
        # AccountNo -> Balance
        self.ID = 0

    @replicated
    def createAccount(self):
        self.ID += 1
        user.incCounter(self.ID)

    def getID(self):
        return self.ID



if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Usage: %s self_port partner1_port partner2_port ...' % sys.argv[0])
        sys.exit(-1)

    userMainPort = int(sys.argv[1])
    accountMainPort = int(sys.argv[1]) + 1000
    userPartners = ['localhost:%d' % int(p) for p in sys.argv[2:]]
    accountPartners = ['localhost:%d' % (int(p) + 1000) for p in sys.argv[2:]]

    user = User('localhost:%d' % userMainPort, userPartners)

    lockManager = ReplLockManager(autoUnlockTime=75) # Lock will be released if connection dropped for more than 75 seconds
    syncObj = SyncObj('localhost:%d' % accountMainPort, accountPartners, consumers=[lockManager])
    
    while True:
        X = 0
        if user._getLeader() is None:
            print("user is None")
            X = 1
        if syncObj._getLeader() is None:
            print("Lock is None")
            X = 1
        
        print(syncObj.getStatus())
        if X:
            time.sleep(1)
            continue

        ip = int(input("Enter: "))
        if ip == 0:
            if lockManager.tryAcquire('testLockName3', sync=True):
                # do some actions
                print("Before incCounter:")
                val = user.incCounter(sync = True)
                print(f"New counter: {val}")
                # lockManager.release('testLockName')
        else:
            print(f"GetCounter: {user.getCounter()}")
        