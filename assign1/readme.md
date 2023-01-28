## STEPS TO RUN
- Install the dependencies: pip install -r requirements.txt

### Set up Postgresql:
- Install using `apt install postgresql postgresql-contrib`
- Check status `service postgresql status`
- login as superuser `sudo -u postgres psql`
- Create Database `create database <database_name>`
- One may also create a custom user define password for it
- To get connection info use `\conninfo`

### Configuration
- Configure the app as follows 
- Existing database : Just set `DB_URI` to database uri and `LOAD_FROM_DB` as True
- Else set `DB_URI` after creating database and set  `LOAD_FROM_DB` as False

### Run
- Run the app from assgn1 directory: flask --app api --debug run
