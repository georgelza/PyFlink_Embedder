#######################################################################################################################
#
#
#   Project             :   Account Holder... Streaming account holder embedding vectorizer 
#
#   File                :   ah_embed_udf.py
#
#   Created             :   8 Dec 2025
#
#   Description         :   Calculate vector values for record arriving in source table, outputting the columns + vector 
#                       :   to a new target table
#                       :
#   Misc Reading        :   https://www.kdnuggets.com/2025/05/confluent/a-data-scientists-guide-to-data-streaming
#                       :   https://www.youtube.com/watch?v=Tn4n9xKE1ug
#                       :   https://www.decodable.co/blog/a-hands-on-introduction-to-pyflink
#
#
#   in Jobmanager i.e.  (NOT SURE)
#
#   /opt/flink/bin/flink run \
#        -m jobmanager:8081 \
#        -py /pyflink/flink_AH_embed_udf.py \
#        -j /opt/flink/lib/flink-sql-connector-postgres-cdc-3.5.0.jar
#
########################################################################################################################
__author__      = "George Leonard"
__email__       = "georgelza@gmail.com"
__version__     = "1.0.0"
__copyright__   = "Copyright 2025, - G Leonard"


from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.udf import udf
from pyflink.table import DataTypes
from pyflink.common import Configuration
import sys

# Lazy import and model loading inside the UDF
import torch
from sentence_transformers import SentenceTransformer

# Embedding model
model      = 'sentence-transformers/all-MiniLM-L6-v2'
dimensions = 384

pipeline_name = f"Flink_ah_embedding"
print(f"Starting {pipeline_name}")

# --------------------------------------------------------------------------
# Some environment settings and a nice Jobname/description "pipeline.name" for flink console
# --------------------------------------------------------------------------

config = Configuration()
config.set_string("table.exec.source.idle-timeout", "1s")
config.set_string(f"pipeline.name", pipeline_name)

# Initialize PyFlink environment
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)  # Adjust based on your cluster

env_settings = EnvironmentSettings \
    .new_instance() \
    .in_streaming_mode() \
    .with_configuration(config) \
    .build()

table_env = StreamTableEnvironment.create(env, environment_settings=env_settings)


# --------------------------------------------------------------------------
# Add necessary JAR dependencies for connectors
# -------------------------------------------------------------------------
postgres_cdc_jar = "file:///opt/flink/lib/flink-sql-connector-postgres-cdc-3.5.0.jar" 
flink_python_jar = "file:///opt/flink/lib/flink-python-1.20.2.jar" 

table_env.get_config().set("pipeline.jars", f"{postgres_cdc_jar};{flink_python_jar}")


# Define UDF for embedding generation
@udf(result_type=DataTypes.ARRAY(DataTypes.FLOAT()))
def generate_ah_embedding(firstname, lastname, dob, gender, children, address, accounts, emailaddress, mobilephonenumber):

    """
    Generate 384-dimensional embeddings for user profiles using sentence-transformers.
    This UDF lazy-loads the model to avoid serialization issues.
    """
    
    # Use a class variable to cache the model across invocations
    if not hasattr(generate_ah_embedding, 'model'):
        
        generate_ah_embedding.model = SentenceTransformer(model)
        generate_ah_embedding.model.eval()  # Set to evaluation mode
    
    # Build a text representation of the user profile
    transaction_parts = []
    
    if firstname:
        transaction_parts.append(f"First name: {firstname}")

    if lastname:
        transaction_parts.append(f"Last name: {lastname}")

    if dob:
        transaction_parts.append(f"Date of birth: {dob}")

    if gender:
        transaction_parts.append(f"Gender: {gender}")

    if children is not None:
        transaction_parts.append(f"Children: {children}")

    if address:
        transaction_parts.append(f"Address: {address}")

    if accounts:
        transaction_parts.append(f"Accounts: {accounts}")

    if emailaddress:
        transaction_parts.append(f"Email: {emailaddress}")

    if mobilephonenumber:
        transaction_parts.append(f"Mobile: {mobilephonenumber}")
    
    # Combine all parts into a single text
    transaction_text = ". ".join(transaction_parts)
    
    # Handle empty account holder profile
    if not transaction_text.strip():
        transaction_text = "Unknown account holder profile"
    
    # Generate embedding
    with torch.no_grad():
        embedding = generate_ah_embedding.model.encode(transaction_text, convert_to_numpy=True)
    
    target_dims = dimensions
    embedding   = embedding[:target_dims]

    # Convert to list of floats for Flink
    return embedding.tolist()

# end generate_embedding


# --------------------------------------------------------------------------
# Our Target table
# Insert into pre-created sink table
# --------------------------------------------------------------------------  
source_table_creation_sql = f"""
    CREATE TABLE postgres_catalog.demog.accountholders (
         _id                BIGINT                  NOT NULL
        ,nationalid         VARCHAR(16)             NOT NULL
        ,firstname          VARCHAR(100)
        ,lastname           VARCHAR(100)
        ,dob                VARCHAR(10) 
        ,gender             VARCHAR(10)
        ,children           INT
        ,address            STRING
        ,accounts           STRING
        ,emailaddress       VARCHAR(100)
        ,mobilephonenumber  VARCHAR(20)
        ,created_at         TIMESTAMP_LTZ(3)
        ,WATERMARK          FOR created_at AS created_at - INTERVAL '15' SECOND
        ,PRIMARY KEY (_id) NOT ENFORCED
    ) WITH (
         'connector'                           = 'postgres-cdc'
        ,'hostname'                            = 'postgrescdc'
        ,'port'                                = '5432'
        ,'username'                            = 'dbadmin'
        ,'password'                            = 'dbpassword'
        ,'database-name'                       = 'demog'
        ,'schema-name'                         = 'public'
        ,'table-name'                          = 'accountholders'
        ,'slot.name'                           = 'accountholders0'
        -- experimental feature: incremental snapshot (default off)
        ,'scan.incremental.snapshot.enabled'   = 'true'               -- experimental feature: incremental snapshot (default off)
        ,'scan.startup.mode'                   = 'initial'            -- https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/flink-sources/postgres-cdc/#startup-reading-position     ,'decoding.plugin.name'                = 'pgoutput'
        ,'decoding.plugin.name'                = 'pgoutput'
    );
"""
table_env.execute_sql(source_table_creation_sql)
statement_set = table_env.create_statement_set()

# --------------------------------------------------------------------------
# Register Apache Paimon output Catalog
# --------------------------------------------------------------------------
table_env.execute_sql("""
    CREATE CATALOG c_paimon WITH (
         'type'                          = 'paimon'
        ,'metastore'                     = 'jdbc'                       -- JDBC Based Catalog
        ,'catalog-key'                   = 'jdbc'
        -- JDBC connection to PostgreSQL for persistence
        ,'uri'                           = 'jdbc:postgresql://postgrescat:5432/flink_catalog?currentSchema=paimon'
        ,'jdbc.user'                     = 'dbadmin'
        ,'jdbc.password'                 = 'dbpassword'
        ,'jdbc.driver'                   = 'org.postgresql.Driver'
        -- MinIO S3 configuration with SSL/TLS (if needed)
        ,'warehouse'                     = 's3://warehouse/paimon'      -- bucket / datastore
        ,'s3.endpoint'                   = 'http://minio:9000'          -- MinIO endpoint
        ,'s3.path-style-access'          = 'true'                       -- Required for MinIO
        -- Default table properties
        ,'table-default.file.format'     = 'parquet'
    );
""")  

table_env.execute_sql("""
    CREATE DATABASE IF NOT EXISTS c_paimon.finflow;
""")  

statement_set = table_env.create_statement_set()
    
# --------------------------------------------------------------------------
# Register the UDF
# --------------------------------------------------------------------------  
table_env.create_temporary_system_function("generate_ah_embedding", generate_ah_embedding)

# --------------------------------------------------------------------------
# Our Target table
# Insert into pre-created sink table
# -------------------------------------------------------------------------- 
# CREATE TABLE c_paimon.finflow.accountholders_embed (
#      _id                           BIGINT
#     ,firstname                     VARCHAR(100)
#     ,lastname                      VARCHAR(100)
#     ,dob                           VARCHAR(10)
#     ,gender                        VARCHAR(10)
#     ,children                      INT
#     ,address                       STRING
#     ,accounts                      STRING
#     ,emailaddress                  VARCHAR(100)
#     ,mobilephonenumber             VARCHAR(20)
#     -- New embedding vector column
#     ,embedding_vector              ARRAY<FLOAT>
#     ,embedding_dimensions          INT
#     ,embedding_timestamp           TIMESTAMP_LTZ(3)    
#     ,created_at                    TIMESTAMP_LTZ(3)
#     ,PRIMARY KEY (_id) NOT ENFORCED
# );


embedding_insert_query = """
    INSERT INTO c_paimon.finflow.accountholders_embed
    SELECT 
         _id
        ,firstname
        ,lastname
        ,dob
        ,gender
        ,children
        ,address
        ,accounts
        ,emailaddress
        ,mobilephonenumber
        ,generate_embedding(
            firstname 
            ,lastname 
            ,dob
            ,gender
            ,children 
            ,address 
            ,accounts
            ,emailaddress
            ,mobilephonenumber
        ) AS embedding_vector
        ,${dimension} AS embedding_dimensions
        ,CURRENT_TIMESTAMP(3) AS embedding_timestamp
        ,created_at
    FROM postgres_catalog.demog.accountholders
"""

# Execute the pipeline
print("Starting accountholders embedding pipeline...")
statement_set.add_insert_sql(embedding_insert_query)
statement_set.execute().wait()

print("Embedding pipeline completed successfully!")