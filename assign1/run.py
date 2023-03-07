from flask_migrate import Migrate

from api import app, db

migrate = Migrate(app, db)


if __name__ == '__main__':
    app.run(debug=True,port=5124)