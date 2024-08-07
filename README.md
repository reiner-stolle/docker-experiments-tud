# Prerequisits
### installed
`docker-compose`
`docker`

# Setting up the imdb database

### Retrieve the latest postgres version**
`docker pull postgres:latest`

### Run this command, replace abs/host/workload with location of job_workload, replace tag with latest**
`docker run -itd -e POSTGRES_PASSWORD=postgres -e PGDATA=/var/lib/postgresql/pgdata --shm-size 10g -p 5432:5432 -v abs/host/workload:/var/lib/postgresql/workload --name postgresql postgres:tag`

### Enter the Docker via bash**
`docker exec -it postgresql bash`

### Restore the db from the sql dump, replace the last argument with the location of imdb_pg11.sql (commonly in var/lib/postgres/)**
`pg_restore -U postgres -d imdb --no-owner --no-privileges job_workload/job_workload/imdb_pg11.sql`

### Enter and check the import
`psql -U postgres imdb`

### Check tables after restore
`\dt`

### Check entries
`select count(*) from title;`

# Retrieve the latest rabbitmq version (as of now rabbitmq==3.13)
### Run rabbitmq
#### for interactive session
`docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.13-management`
#### for detached session (runs in background)
`docker run -d --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.13-management`

### create Network for Docker-compose
`docker network create my_network`

### connect postgresql to my_network
`docker network connect my_network postgresql`

### (optional) copy job_queries into user container
`docker cp ./job_queries user:/`
