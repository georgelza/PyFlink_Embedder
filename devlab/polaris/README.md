## Polaris based Catalog, configured with PostgreSQL persistent store

For our [Apache Iceberg](https://iceberg.apache.org) based tables we'll be using a [Apache Polaris](https://polaris.apache.org) as our Catalog store.

The basic verification/qualification of our build can be found in `docker-compose-basic.yaml`, this can be started up on it's own using `make run_basic`. The catlaog has been configured to use PostgreSQL as persistant store.

This is provided via the `postgrecat` service that spins up a PostgreSQL server and creates a `catalog_store` database. Polaris as part of the bootstrap them creates a schema: `polaris_schema` which is utilised for the required tables.

See the `.env` and the below specific variables.

```bash
# PostgreSQL Catalog Store
#
COMPOSE_PROJECT_NAME        = aiembed
REPO_NAME                   = georgelza

# PostgreSQL CDC Source
POSTGRES_CDC_HOST=postgrescdc
POSTGRES_CDC_PORT=5433
POSTGRES_CDC_USER=dbadmin
POSTGRES_CDC_PASSWORD=dbpassword
POSTGRES_CDC_DB=demog

# PostgreSQL Catalog Store used for out Polaris REST catalog for our Iceber tables & Flink JDBC catalog used for Paimon tables
#
# If you decide to change the CAT_DB name then also update <Project Root>/devlab/sql/postgrescat/postgresql-init.sql script, 
# making sure to assign dbadmin user access to the new DB name chosen.
#
# Following strict Polaris conventions the POSTGRES_CAT_DB name should match the value for POLARIS_REALM variable
# We create flink_catalog as the default database, then create findept using the postgresql-init.sql script.
POSTGRES_CAT_HOST=postgrescat
POSTGRES_CAT_PORT=5432
POSTGRES_CAT_USER=dbadmin
POSTGRES_CAT_PASSWORD=dbpassword
POSTGRES_CAT_DB=flink_catalog

# Polaris Catalog
ROOT_CLIENT_ID=root
ROOT_CLIENT_SECRET=s3cr3t
CATALOG_NAME=icebergcat
POLARIS_REALM=findept

# Minio
MINIO_ROOT_USER=mnadmin
MINIO_ROOT_PASSWORD=mnpassword
MINIO_ALIAS=minio
MINIO_ENDPOINT=http://minio:9000
MINIO_BUCKET=warehouse

AWS_ACCESS_KEY_ID=mnadmin
AWS_SECRET_ACCESS_KEY=mnpassword
AWS_REGION=za-south-1
AWS_DEFAULT_REGION=za-south-1

```

## Apache Polaris Resources

For more "insight" or is that exploring this rabbit hole.

- [Polaris](https://polaris.apache.org)

- [Quick Start with Apache Iceberg and Apache Polaris on your Laptop (quick setup notebook environment)](https://www.dremio.com/blog/quick-start-with-apache-iceberg-and-apache-polaris-on-your-laptop-quick-setup-notebook-environment/)

- Also various examples at the Project GIT Repo: [Polaris](https://github.com/apache/polaris.git) in the `getting-started` sub directory.


## Deployment Steps

### 1. Start the Services

```bash
# Start all services
make run_basic

# Check service status
make ps
# or
docker-compose ps

# View logs
docker-compose logs -f polaris
docker-compose logs -f postgrescat
docker-compose logs -f minio

# or

make logsf |grep polaris
make logsf |grep postgrescat
make logsf |grep minio

```


### 2. Verify PostgreSQL Setup

```bash
# Connect to PostgreSQL  => Polaris Data
docker exec -it postgrescat psql -U dbadmin -d findept

# List all schemas
\dn

# Expected output:
#          Name          |  Owner  
# -----------------------+---------
#  polaris_schema        | dbadmin
#  public                | dbadmin

# Exit
\q

# Connect to PostgreSQL  => Flink JDBC Data
docker exec -it postgrescat psql -U dbadmin -d flink_catalog

# List all schemas
\dn

# Expected output:
#          Name          |  Owner  
# -----------------------+---------
#  paimon_catalog        | dbadmin
#  public                | dbadmin

# Exit
\q

```


### 3. Check Polaris health

```bash
curl http://localhost:8182/q/health

curl -w "\nHTTP Status: %{http_code}\n" http://localhost:8182/healthcheck

curl -f http://localhost:8182/q/metrics

docker inspect polaris --format='{{.State.Health.Status}}'
```


### 4. Extract Polaris Credentials

```bash
# Extract credentials from .env
ROOT_CLIENT_ID=$(grep ROOT_CLIENT_ID .env | cut -d '=' -f2)
ROOT_CLIENT_SECRET=$(grep ROOT_CLIENT_SECRET .env | cut -d '=' -f2)

echo "Client ID: ${ROOT_CLIENT_ID}"
echo "Client Secret: ${ROOT_CLIENT_SECRET}"
```

**Your Credentials:**
- Client ID: `root`
- Client Secret: `s3cr3t`


### 5. Get OAuth Token from Polaris

```bash
# Get access token
#
# Retrieve access token - This needs to be done before any other commands are executed as the TOKEN forms part of the API call variables.
# Note the client_id and client_secret need to match .env/ROOT_CLIENT_* values

export TOKEN=$(curl -s -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
    -d 'grant_type=client_credentials' \
    -d 'client_id=root' \
    -d 'client_secret=s3cr3t' \
    -d 'scope=PRINCIPAL_ROLE:ALL' \
    | jq -r '.access_token')

echo "Token: ${TOKEN}"
```


### 6. Create our Polaris Catalogs (Optional via REST API)

The below was execute/completed via our bootstrap service defined in our `docker-compose.yaml` file.

Take note of our helper scripts located in `<Project Root>/conf/polaris`.

```bash
# The below catalog is deployed as part of our `polaris-setup` docker-compose service
#
# Create (iceberg based) catalog via REST API (optional - Flink will create via REST)
# We specify a "specified" folder location under our MinIO warehouse root

curl -X POST http://localhost:8181/api/management/v1/catalogs \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "icerbergcat",
    "type": "INTERNAL",
    "properties": {
      "default-base-location": "s3://warehouse/iceberg"
    },
    "storageConfigInfo": {
      "storageType": "S3",
      "pathStyleAccess": true,
      "allowedLocations": ["s3://warehouse/iceberg"]
    }
}' | jq
```

```bash
# Create (paimon based) catalog via REST API (optional - Flink will create via REST)
# We specify a "specified" folder location under our MinIO warehouse root

curl -X POST http://localhost:8181/api/management/v1/catalogs \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "paimoncat",
    "type": "INTERNAL",
    "properties": {
      "default-base-location": "s3://warehouse/paimon"
    },
    "storageConfigInfo": {
      "storageType": "S3",
      "pathStyleAccess": true,
      "allowedLocations": ["s3://warehouse/paimon"]
    }
}' | jq

# List configured catalogs
curl -X GET http://localhost:8181/api/management/v1/catalogs \
  -H "Authorization: Bearer ${TOKEN}" | jq
```


### 7. Create Catalog Namespace 

When creating a database using flink-sql inside a catalog that will also create a namespace in the "warehouse" property.

```bash
# 1. Create 'fraud' Namespace inside icebergcat catalog
curl -X POST http://localhost:8181/api/catalog/v1/icebergcat/namespaces \
    -H "Authorization: Bearer ${TOKEN}" \
    -H 'Content-Type: application/json' \
    -d '{"namespace": ["fraud"], "properties": {"description": "Iceberg catalog database"}}' | jq

curl -X GET http://localhost:8181/api/catalog/v1/icebergcat/namespaces \
  -H "Authorization: Bearer $TOKEN" | jq


# 2. Create 'finflow' Namespace inside paimoncat catalog
curl -X POST http://localhost:8181/api/catalog/v1/paimoncat/namespaces \
    -H "Authorization: Bearer ${TOKEN}" \
    -H 'Content-Type: application/json' \
    -d '{"namespace": ["finflow"], "properties": {"description": "Paimon catalog database"}}' | jq

curl -X GET http://localhost:8181/api/catalog/v1/paimoncat/namespaces \
  -H "Authorization: Bearer $TOKEN" | jq
```


### 8. Flink SQL Catalog Setup

```sql
-- 1. Create c_iceberg catalog (Polaris REST)
CREATE CATALOG c_iceberg WITH (
   'type'='iceberg'
  ,'catalog-type'='rest'
  ,'uri'='http://polaris:8181/api/catalog'
  ,'warehouse'='icerbergcat'
  ,'oauth2-server-uri'='http://polaris:8181/api/catalog/v1/oauth/tokens'
  ,'credential'='root:s3cr3t'
  ,'scope'='PRINCIPAL_ROLE:ALL'
  ,'s3.endpoint'='http://minio:900'
  ,'s3.access-key-id'='mnadmin'
  ,'s3.secret-access-key'='mnpassword'
  ,'s3.path-style-access'='true'
);

USE CATALOG c_iceberg;
CREATE DATABASE IF NOT EXISTS finflow;


-- 2. Create c_paimon catalog (Polaris REST)
CREATE CATALOG c_paimon WITH (
   'type'='paimon'
  ,'catalog-type'='rest'
  ,'uri'='http://polaris:8181/api/catalog'
  ,'warehouse'='paimoncat'
  ,'oauth2-server-uri'='http://polaris:8181/api/catalog/v1/oauth/tokens'
  ,'credential'='root:s3cr3t'
  ,'scope'='PRINCIPAL_ROLE:ALL'
  ,'s3.endpoint'='http://minio:900'
  ,'s3.access-key-id'='mnadmin'
  ,'s3.secret-access-key'='mnpassword'
  ,'s3.path-style-access'='true'
);

USE CATALOG c_paimon;
CREATE DATABASE IF NOT EXISTS finflow;


-- 3. Create CDC source tables in default_catalog
-- Note: These are NOT a catalog, just table definitions with postgres-cdc connector
USE CATALOG default_catalog;
CREATE DATABASE IF NOT EXISTS demog;
USE demog;

-- 3. Create CDC tables
-- See full script for details, <Project Toor>/devlab/creFlinkFlows/2.1.creCdc.sql

```

## Database, Schema, Catalog Structure

PostgreSQL Server: `postgrescat`

**Note**: also shown is the presence of the Flink based JDBC catalog database: `flink_catalog` hosted on the same PostgreSQL server.


### 1. DB: findept

| Schema | Purpose | Used By |
|--------|---------|---------|
| `polaris_schema` | Polaris metadata storage | Apache Polaris (Iceberg) |
| `public`  | Default PostgreSQL schema | General use |

### 2. DB: flink_catalog

| Schema | Purpose | Used By |
|--------|---------|---------|
| `paimon_catalog` | Flink metadata storage | Apache Flink JDBC Catalog (Paimon) |
| `public`  | Default PostgreSQL schema | General use |


### 3. Catalog Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     Flink SQL Engine                    │
└─────────────────────────────────────────────────────────┘
         │                  │                    │
         │                  │                    │
    ┌────▼────┐       ┌─────▼─────┐        ┌─────▼──────────┐
    │c_iceberg│       │ c_paimon  │        │ default_catalog│
    │ (REST)  │       │  (JDBC)   │        │ (in-memory)    │
    └────┬────┘       └─────┬─────┘        └──────┬─────────┘
         │                  │                     │
         │                  │                     │
         └──────┐           └─┐                   │
                │             │                   │
           ┌────▼────┐   ┌────▼────┐        ┌─────▼──────┐
           │ Polaris │   │ Flink   │        │demog DB:   │
           │  REST   │   │  JDBC   │        │CDC table   │
           │  API    │   │         │        │definitions │
           └────┬────┘   └────┬────┘        │(reference  │
                │             │             │PostgresCDC)│
                │             │             └────────────┘
           ┌────▼─────────────▼───────────────────┐
           │       PostgresCAT Pg Server          │
           │                                      │
           │  DB:findept         DB:flink_catalog │
           │  polaris_schema     paimon_catalog   │
           │  (polaris REST      (JDBC based      │
           │   based catalog)     catalog)        │
           └────┬───────────────────┬─────────────┘
                │                   │
                │                   │
           ┌────▼───────────────────▼─────┐
           │      MinIO S3 Storage        │
           │                              │
           │     warehouse/iceberg/       │
           │     warehouse/paimon/        │
           └────┬───────────────────┬─────┘
                │                   │
                │                   │
           ┌────▼───────────────────▼───────┐
           │           -> c_iceberg         │
           │     warehouse/iceberg/finflow  │
           │     warehouse/iceberg/fraud    │
           │                                │
           │           -> c_paimon          │
           │     warehouse/paimon/finflow   │
           └────────────────────────────────┘

```

### 4. MinIO Storage Structure

```
warehouse/
│
├── iceberg/          # c_iceberg Iceberg based catalog housing Iceberg table data and metadata
│   └── finflow/      # finflow database/namespace
│   └── fraud/        # fraud database/namespace
│
└── paimon/           # c_paimon Paimon based catalog housing Paimon table data and metadata
    └── finflow/      # finflow database/namespace

```

**Key Points:**

- c_iceberg uses Apache Polaris REST catalog interface
  
- Apache Polaris stores metadata in `polaris_schema` schema

- c_paimon uses Apache Flink JDBC based catalog interface

- Apache Flink stores metadata in `paimon_catalog` schema

- CDC tables are just connector definitions, not a catalog
- CDC tables reference external PostgreSQL database directly