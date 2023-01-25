from api import db


class QueueDB(db.model):
    '''
    Id : PK int
    Nxt_id: int
    Value: string
    Topic: string as FK
    '''
    id = db.Column(db.Integer,primary_key=True)
    nxt_id = db.Column(db.Integer)
    value = db.Column(db.String,nullable=False)
    
class Topics(db.model):
    id = db.Column(db.Integer,primary_key=True)
    start_ind = db.Column(db.Integer,nullable=True)
    end_ind = db.Column(db.Integer,nullable=True)
    producers = db.relationship('Producer',backref='topic',lazy=True)
    consumers = db.relationship('Consumer',backref='topic',lazy=True)


class Producer(db.model):
    id = db.Column(db.Integer,primary_key=True)
    topic_id = db.Column(db.Integer,db.ForeignKey('topic.id'),nullable=False)

class ConsumerSub(db.model):
    id = db.Column(db.Integer,primary_key=True)
    offset = db.Column(db.Intger,nullable=False)
    topic_id = db.Column(db.Integer,db.ForeignKey('topic.id'),nullable=False)
    c_id = db.Column(db.Integer,db.ForeignKey('consumer.id'),nullable=False)

class Consumer(db.model):
    id = db.Column(db.Integer,primary_key = True)
    subs = db.relationship('Sub',backref='consumer',lazy=True)

