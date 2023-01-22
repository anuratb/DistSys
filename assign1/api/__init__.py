import psycopg2
from sqlalchemy import create_engine 
from sqlalchemy.orm import scoped_session, sessionmaker
DB_URI = 'postgresql+psycopg2://anurat:abcd@127.0.0.1:5432/anurat'
engine = create_engine(DB_URI) 
db = scoped_session(sessionmaker(bind=engine))

connection = engine.connect() 
#result = connection.execute('select * from table') 
#print(connection)