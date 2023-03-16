## STEPS TO RUN
- Install the dependencies: pip install -r requirements.txt

### Set up Postgresql:
- Install using `apt install postgresql postgresql-contrib`
- Check status `service postgresql status`
- login as postgres `sudo -i -u postgres`
- Create Role `createuser --interactive`
- login as superuser `sudo -u postgres psql`
- Create Database `create database <database_name>`
- Login as `sudo -i -u <usernmae> psql`
- One may also create a custom user define password for it using `\password`
- To get connection info use `\conninfo`

### Configuration
- Set the environment variable in .env
DB_USERNAME : Database Username
DB_PASSWORD : Database PAssword
DB_HOST : Database host for manager
DB_PORT : Database port for manager
DOCKER_IMG_BROKER : Broker Docker Image
DOCKER_IMG_MANAGER : Manager Docker image

POSTGRES_CONTAINER : Manager DB container
DB_NAME= Manager Database Name
LOAD_FROM_DB : Set true to load from existing DB
NUMBER_OF_BROKERS : Number of Brokers
EXECUTE='0'
NUMBER_READ_MANAGERS : Number of read managers

RANDOM_SEED=42
HEART_BEAT_INTERVAL=0.1

### Run
- Run the app from assgn1 directory: `python3 run.py` in manage_brokers
