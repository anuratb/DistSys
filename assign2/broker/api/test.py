from api.data_struct import TopicNode,Queue
from api.models import QueueDB,Topics,Producer,Consumer



def run_tests():
    Queue.createTopic("T1")
    Queue.createTopic("T2")
    Queue.createTopic("T3")
    topic_list = Queue.listTopics()
    print(topic_list)









if __name__=='__main__':
    run_tests()