## Setup of the source database

With docker compose installed simply run:

```
docker-compose up
```

You can find the credentials at the docker-compose.yml file


## Create the target postgres server

Use pgAdmin to create the destination server, make sure that:

host     = "localhost"
port     = "5433"
user     = "postgres"
password = "0102"


## Create the target postgres database

Use pgAdmin to create the destination database as in:

```
CREATE DATABASE db_lh_challenge
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'Portuguese_Brazil.1252'
    LC_CTYPE = 'Portuguese_Brazil.1252'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;
```

## execute the Python script

Use your terminal to access code-challenge folder and run etl.py

It will prompt you to enter a date


## Query the final database

Use the following sql script to show the orders and its details 

```
SELECT * FROM order_details od INNER JOIN orders o ON OD.order_id = O.order_id
```