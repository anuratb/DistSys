#!/usr/bin/env python
from __future__ import print_function

import sys
import time
from functools import partial
sys.path.append("../")
from pysyncobj import SyncObj, replicated

user = None
account = None

class User(SyncObj):

    def __init__(self, selfNodeAddr, otherNodeAddrs):
        super(User, self).__init__(selfNodeAddr, otherNodeAddrs)
        # UserName -> List of Accounts
        self.userList = {}

    @replicated
    def addUser(self, userName, accountNo):
        if userName not in self.userList:
            self.userList[userName] = []
        self.userList[userName].append(accountNo)
    
    def checkValidity(self, userName, accountNo):
        if userName not in self.userList.keys():
            return False
        if accountNo not in self.userList[userName]:
            return False
        return True
    
    def getUserList(self):
        print(f"## userList: {self.userList}")
