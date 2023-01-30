from api import db


class QueueDB(db.Model):
    '''
    Id : PK int
    Nxt_id: int
    Value: string
    Topic: string as FK
    '''
    id = db.Column(db.Integer,primary_key=True)
    nxt_id = db.Column(db.Integer)
    value = db.Column(db.String,nullable=False)
    
class Topics(db.Model):
    id = db.Column(db.Integer,primary_key=True)
    value = db.Column(db.String,primary_key = False,nullable=False)
    start_ind = db.Column(db.Integer,nullable=True)
    end_ind = db.Column(db.Integer,nullable=True)
    producers = db.relationship('Producer',backref='topic',lazy=True)
    consumers = db.relationship('Consumer',backref='topic',lazy=True)


class Producer(db.Model):
    id = db.Column(db.Integer,primary_key=True)
    topic_id = db.Column(db.Integer,db.ForeignKey('topics.id'))

class Consumer(db.Model):
    id = db.Column(db.Integer,primary_key=True)
    offset = db.Column(db.Integer,nullable=False)
    topic_id = db.Column(db.Integer,db.ForeignKey('topics.id'))
    

