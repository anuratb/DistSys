IP=$1
user=$2
pass=$3
DB_NAME=$5
self_addr=$6
partner_addr=$7
docker run --network host --publish-all -it -e DB_URI="postgresql+psycopg2://$user:$pass@$IP:5432/$DB_NAME" -e BROKER_ID=$4 -e SELF_ADDR=$6 -e SLAVE_ADDR=$7 broker