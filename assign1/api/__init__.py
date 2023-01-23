

from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import os
DB_URI = 'postgresql+psycopg2://anurat:abcd@127.0.0.1:5432/anurat'
cnt = 0
app = Flask(__name__)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SECRET_KEY'] = '5791628bb0b13ce0c676dfde280ba245'
app.config['SQLALCHEMY_DATABASE_URI'] = DB_URI
db = SQLAlchemy(app)
from api import routes



