Setting up the imdb database

### **Retrieve the latest postgres version**
`docker pull postgres:latest`

### **Run this command, replace abs/host/workload with location of job_workload**
`docker run -itd -e POSTGRES_PASSWORD=postgres -e PGDATA=/var/lib/postgresql/pgdata --shm-size 10g -p 5432:5432 -v abs/host/workload:/var/lib/postgresql/workload --name postgresql postgres:tag`
