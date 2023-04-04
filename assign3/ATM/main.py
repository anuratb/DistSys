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


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Usage: %s self_port partner1_port partner2_port ...' % sys.argv[0])
        sys.exit(-1)

    userMainPort = int(sys.argv[1])
    accountMainPort = int(sys.argv[1]) + 1000
    userPartners = ['localhost:%d' % int(p) for p in sys.argv[2:]]
    accountPartners = ['localhost:%d' % (int(p) + 1000) for p in sys.argv[2:]]

    user = User('localhost:%d' % userMainPort, userPartners)
    account = Account('localhost:%d' % accountMainPort, accountPartners)

    while True:
        if user._getLeader() is None or account._getLeader() is None:
            continue
        print("\n===============================================")
        print("Enter 1 to create Account")
        print("Enter 2 for withdrawal")
        print("Enter 3 for deposit")
        print("Enter 4 for balance enquiry")
        print("Enter 5 for transfering funds to other account")
        print("===============================================\n")

        ip = int(input("Enter your choice: "))
        
        if ip < 1 or ip > 5:
            user.getUserList()
            account.getAllAccounts()
            print("## Error: Wrong choice entered...")
        else:
            userName = input("Enter user name: ")
            if ip == 1:
                accountNo = account.createAccount(sync=True)
                user.addUser(userName, accountNo)
                print(f"$$ Account created successfully. Your Account Number: {accountNo}")
            else:
                accountNo = input("Enter your account number: ")

                if not user.checkValidity(userName, accountNo):
                    print("## Error: User is not registered with account")
                    continue

                if ip == 2:
                    amount = int(input("Enter withdrawal amount: "))
                    resp = account.withDrawal(accountNo, amount, sync = True)
                    print(resp)   
                elif ip == 3:
                    amount = int(input("Enter deposit amount: "))
                    resp = account.deposit(accountNo, amount, sync = True)
                    print(resp)
                elif ip == 4:
                    resp = account.balanceEnquiry(accountNo)
                    print(f"$$ Current Balance: {resp}")
                else:
                    accountTo = input("Enter the account number for fund transfer: ")
                    amount = int(input("Enter the amount to transfer: "))
                    resp = account.fundTransfer(accountTo, accountNo, amount, sync = True)
                    print(resp)