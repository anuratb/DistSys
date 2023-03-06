import os
import subprocess
import psycopg2
import time
def get_url(container:str):
    obj = subprocess.Popen("docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "+container, shell=True, stdout=subprocess.PIPE).stdout.read()
    url =  obj.decode('utf-8').strip() 
    return url
def create_postgres_db(db_name:str,container_name:str,user,password,postgres_img="postgres"):
    #os.system("docker rm -f {}".format(container_name))
    cmd = "docker run -d --expose 5432 --name {} -e POSTGRES_PASSWORD={} -e POSTGRES_USER={} -p 0:5432 -v /data:/var/lib/postgresql/data --rm {}".format(container_name,password,user,postgres_img)
    os.system(cmd)
    
    url = get_url(container_name)

    while(True):
        time.sleep(1)
        res = os.system("ping -c 1 "+url)
        if(res == 0):
            break
    
    ############## Create Database #######################
    while(True):
        try:
            conn = psycopg2.connect(
                user=user, password=password, host=url, port= 5432
            )
            break
        except:
            continue
    conn.autocommit = True

    cursor = conn.cursor()
    cursor.execute("SELECT datname FROM pg_database;")

    list_database = cursor.fetchall()
    list_database = [itr[0] for itr in list_database]
    if(db_name in list_database):
        cursor.execute('DROP DATABASE {} WITH (FORCE)'.format(db_name))
    sql = '''CREATE database {};'''.format(db_name)
    cursor.execute(sql)
    
    conn.close()
    ####################################################
    db_uri = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(user,password,url,5432,db_name)
    return db_uri
def create_container(db_uri:str,container_name:str,img,envs={},expose_port=5124):
    os.system("docker rm -f {}".format(container_name))
    cmd = "docker run --name {} -d -p 0:{} --expose {} -e DB_URI={} ".format(container_name,expose_port,expose_port,db_uri)
    for key,val in envs.items():
        cmd+=' -e {}={} '.format(str(key),str(val))
    cmd+=(" --rm "+img)
    os.system(cmd)
    url = get_url(container_name)
    while(True):
        time.sleep(1)
        res = os.system("ping -c 1 "+url)
        if(res == 0):
            break
    url = 'http://' + url + ':'+str(expose_port)
    return url
def is_server_running(url):
    return os.system("ping -c 1 "+url)==0