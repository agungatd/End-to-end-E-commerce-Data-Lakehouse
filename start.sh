#!/bin/bash
set -e

log() {
    local filename="${BASH_SOURCE[0]##*/}"
    local lineno="${BASH_LINENO[0]}"
    echo "[$(date +"%Y-%m-%d, %H:%M:%S %Z")] {${filename}:${lineno}} INFO - $*"
}

# Validate docker and docker compose is installed
if ! command -v docker-compose &> /dev/null || ! command -v docker &> /dev/null; then
    echo "docker-compose command could not be found. Please install docker and docker compose"
    echo "run ./install-docker.sh in this project root directory to install both docker and docker compose"
    echo "or go to https://docs.docker.com/get-docker/ for installation instructions"
    exit 1
fi

# Check if .env file exists
if [ -f ".env" ]; then
    log ".env file exists."
else
    log ".env file does not exist. please rename .temp.env file to .env after changing the values"
fi

# make .env values accessible from any project directory.
set -a
. ./.env
set +a


log "remove running containers"
# docker compose down -v
docker compose down

log "run all services from docker compose"
# docker compose --profile lakehouse up -d --build
docker compose --profile lakehouse up -d

sleep 5
# log "Copy postgres jar, for migrating data from postgres to data lakehouse"
# docker cp ./spark/jars/postgresql-42.7.4.jar spark-iceberg:/opt/spark/jars/

log "Run initial tables creation using iceberg"
docker cp ./spark/sql/ spark-iceberg:/home/spark/
docker exec spark-iceberg spark-sql -f /home/spark/sql/create_tables.sql

log "Copy spark scripts & run migrating job from postgres to minio using iceberg"
# docker cp ./spark/jobs/ spark-iceberg:/home/spark/
docker exec spark-iceberg spark-submit /home/spark/jobs/migrate_pg_datalake.py

log "Set ssh for spark-iceberg container."
echo -e "$SPARK_ICEBERG_PASSWD\n$SPARK_ICEBERG_PASSWD" | docker exec -i spark-iceberg passwd
docker exec spark-iceberg sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/g' /etc/ssh/sshd_config
docker exec spark-iceberg service ssh restart

log "Set StrictHostKeyChecking in airflow-scheduler for spark-iceberg ssh"
echo -e "$SPARK_ICEBERG_PASSWD" | docker exec -u 0 -i airflow-scheduler ssh -o StrictHostKeyChecking=accept-new root@spark-iceberg

log "Done!"