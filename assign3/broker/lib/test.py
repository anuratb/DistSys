from library.lib import MyQueue

URL = 'http://127.0.0.1:5124'

def run_tests():
    # Runnig multiple times error
    try:
        Q = MyQueue(URL)
    except Exception as err:
        print(err)
    try:
        Q.createTopic('T111')
    except Exception as err:
        print(err)
    try:
        Q.createTopic('T211')
    except Exception as err:
        print(err)
    try:
        Q.createTopic('T3114')
    except Exception as err:
        print(err)
    try:
         topic_list_recv = Q.get_all_topics()
         print(topic_list_recv)
    except Exception as err:
         print(err)

    try:
        p1 = Q.createProducer(['T11','T12','T13'])
        print(p1)
    except Exception as err:
        print(err)


    


if __name__=='__main__':
    run_tests()