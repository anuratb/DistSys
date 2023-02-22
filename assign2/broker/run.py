from flask_migrate import Migrate

#print(api.app.app,api.app.db)
from api import app,db
migrate = Migrate(app, db)



if __name__ == '__main__':
    app.run(debug=True,port=5123)