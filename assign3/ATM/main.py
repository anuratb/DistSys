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

    
class Account(SyncObj):

    def __init__(self, selfNodeAddr, otherNodeAddrs):
        super(Account, self).__init__(selfNodeAddr, otherNodeAddrs)
        # AccountNo -> Balance
        self.accountList = {}
        self.ID = 0

    @replicated
    def createAccount(self):
        ID_ = str(self.ID)
        self.ID += 1
        self.accountList[ID_] = 0
        return ID_

    @replicated
    def withDrawal(self, accountNo, amount):
        if amount > self.accountList[accountNo]:
            return "## Error: Not enough Balance!"
        self.accountList[accountNo] -= amount
        return f"$$ Updated balance: {self.accountList[accountNo]}"
    
    @replicated
    def deposit(self, accountNo, amount):
        self.accountList[accountNo] += amount
        return f"$$ Updated balance: {self.accountList[accountNo]}"

    @replicated
    def fundTransfer(self, accountTo, accountFrom, amount):
        if accountTo not in self.accountList:
            return f"## Error: Account Number {accountTo} doesn't exist"
        if amount > self.accountList[accountFrom]:
            return "## Error: Not enough Balance!"
        self.accountList[accountFrom] -= amount
        self.accountList[accountTo] += amount
        return f"$$ Transfer successful. Updated Balance: {self.accountList[accountFrom]}"

    def balanceEnquiry(self, accountNo):
        return self.accountList[accountNo]

    def getAllAccounts(self):
        print(f"## accountList: {self.accountList}")
        print(f"## Latest Account ID: {self.ID}")