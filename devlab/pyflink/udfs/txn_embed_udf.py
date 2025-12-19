#######################################################################################################################
#
#
#   Project             :   Transaction... Streaming transaction embedding vectorizer 
#
#   File                :   txn_embed_udf.py
#
#   Created             :   8 Dec 2025
#
#   Description         :   Calculate vector values for record arriving in source table, outputting the columns + vector 
#                       :   to a new target table
#
#   Misc Reading        :   https://www.kdnuggets.com/2025/05/confluent/a-data-scientists-guide-to-data-streaming
#                       :   https://www.youtube.com/watch?v=Tn4n9xKE1ug
#                       :   https://www.decodable.co/blog/a-hands-on-introduction-to-pyflink
#
#
#   in Jobmanager i.e.  (NOT SURE)
#
#   /opt/flink/bin/flink run \
#        -m jobmanager:8081 \
#        -py /pyflink/udfs/txn_embed_udf.py \
#        -j /opt/flink/lib/flink-sql-connector-postgres-cdc-3.5.0.jar
#
########################################################################################################################
__author__      = "George Leonard"
__email__       = "georgelza@gmail.com"
__version__     = "1.0.0"
__copyright__   = "Copyright 2025 - G Leonard"


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

# sentence-transformers/all-mpnet-base-v2   (768D) - Higher quality slower
# BAAI/bge-small-en-v1.5                    (384D) - Optimized for retrieval tasks
# intfloat/e5-small-v2                      (384D) - Good for semantic similarity

# Define UDF for embedding generation
@udf(result_type=DataTypes.ARRAY(DataTypes.FLOAT()))
def generate_txn_embedding(eventtime, direction, eventtype, creationdate
        ,accountholdernationalid, accountholderaccount
        ,counterpartynationalid, counterpartyaccount
        ,tenantid, fromid, accountagentid, fromfibranchid
        ,accountnumber, toid, accountidcode, counterpartyagentid
        ,tofibranchid, counterpartynumber, counterpartyidcode
        ,amount, msgtype, settlementclearingsystemcode
        ,paymentclearingsystemreference, requestexecutiondate
        ,settlementdate, destinationcountry, localinstrument
        ,msgstatus, paymentmethod, settlementmethod
        ,transactiontype, verificationresult, numberoftransactions
        ,schemaversion, usercode):
    
    """
    Generate 384-dimensional embeddings for financial transactions using sentence-transformers.
    This UDF lazy-loads the model to avoid serialization issues.
    
    Excludes fields: eventid, transactionid, embedding_timestamp and embedding_dimensions, created_at from embedding generation
    """
    
    # Use a class variable to cache the model across invocations
    if not hasattr(generate_txn_embedding, 'model'):
        
        generate_txn_embedding.model = SentenceTransformer(model)
        generate_txn_embedding.model.eval()  # Set to evaluation mode
    
    
    # Build a structured text representation of the transaction
    transaction_parts = []
    
    if eventtime:
        transaction_parts.append(f"Event time: {eventtime}")
    
    if direction:
        transaction_parts.append(f"Direction: {direction}")
    
    if eventtype:
        transaction_parts.append(f"Event type: {eventtype}")
    
    if creationdate:
        transaction_parts.append(f"Creation date: {creationdate}")
    
    if accountholdernationalid:
        transaction_parts.append(f"Account holder national ID: {accountholdernationalid}")
    
    if accountholderaccount:
        transaction_parts.append(f"Account holder account: {accountholderaccount}")
    
    if counterpartynationalid:
        transaction_parts.append(f"Counterparty national ID: {counterpartynationalid}")
    
    if counterpartyaccount:
        transaction_parts.append(f"Counterparty account: {counterpartyaccount}")
    
    if tenantid:
        transaction_parts.append(f"Tenant ID: {tenantid}")
    
    if fromid:
        transaction_parts.append(f"From ID: {fromid}")
    
    if accountagentid:
        transaction_parts.append(f"Account agent ID: {accountagentid}")
    
    if fromfibranchid:
        transaction_parts.append(f"From FI branch ID: {fromfibranchid}")
    
    if accountnumber:
        transaction_parts.append(f"Account number: {accountnumber}")
    
    if toid:
        transaction_parts.append(f"To ID: {toid}")
    
    if accountidcode:
        transaction_parts.append(f"Account ID code: {accountidcode}")
    
    if counterpartyagentid:
        transaction_parts.append(f"Counterparty agent ID: {counterpartyagentid}")
    
    if tofibranchid:
        transaction_parts.append(f"To FI branch ID: {tofibranchid}")
    
    if counterpartynumber:
        transaction_parts.append(f"Counterparty number: {counterpartynumber}")
    
    if counterpartyidcode:
        transaction_parts.append(f"Counterparty ID code: {counterpartyidcode}")
    
    if amount:
        transaction_parts.append(f"Amount: {amount}")
    
    if msgtype:
        transaction_parts.append(f"Message type: {msgtype}")
    
    if settlementclearingsystemcode:
        transaction_parts.append(f"Settlement clearing system code: {settlementclearingsystemcode}")
    
    if paymentclearingsystemreference:
        transaction_parts.append(f"Payment clearing system reference: {paymentclearingsystemreference}")
    
    if requestexecutiondate:
        transaction_parts.append(f"Request execution date: {requestexecutiondate}")
    
    if settlementdate:
        transaction_parts.append(f"Settlement date: {settlementdate}")
    
    if destinationcountry:
        transaction_parts.append(f"Destination country: {destinationcountry}")
    
    if localinstrument:
        transaction_parts.append(f"Local instrument: {localinstrument}")
    
    if msgstatus:
        transaction_parts.append(f"Message status: {msgstatus}")
    
    if paymentmethod:
        transaction_parts.append(f"Payment method: {paymentmethod}")
    
    if settlementmethod:
        transaction_parts.append(f"Settlement method: {settlementmethod}")
    
    if transactiontype:
        transaction_parts.append(f"Transaction type: {transactiontype}")
    
    if verificationresult:
        transaction_parts.append(f"Verification result: {verificationresult}")
    
    if numberoftransactions is not None:
        transaction_parts.append(f"Number of transactions: {numberoftransactions}")
    
    if schemaversion is not None:
        transaction_parts.append(f"Schema version: {schemaversion}")
    
    if usercode:
        transaction_parts.append(f"User code: {usercode}")
    
    # Combine all parts into a single text
    transaction_text = ". ".join(transaction_parts)
    
    # Handle empty transaction
    if not transaction_text.strip():
        transaction_text = "Unknown transaction"
    
    # Generate embedding
    with torch.no_grad():
        embedding = generate_txn_embedding.model.encode(transaction_text, convert_to_numpy=True)

    target_dims = dimensions
    embedding   = embedding[:target_dims]
    
    # Convert to list of floats for Flink
    return embedding.tolist()

# end generate_txn_embedding

def main():

    pipeline_name = f"Flink_txn_embedding"

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

    # --------------------------------------------------------------------------
    # Register Postgres Input Catalog
    # --------------------------------------------------------------------------
    table_env.execute_sql("""
        CREATE CATALOG postgres_catalog WITH 
        ('type'='generic_in_memory'); 
    """)  

    
    # --------------------------------------------------------------------------
    # Create the source table
    # --------------------------------------------------------------------------
    source_table_creation_sql = f"""
        CREATE OR REPLACE TEMPORARY TABLE postgres_catalog.demog.transactions (
             _id                            BIGINT              NOT NULL
            ,eventid                        VARCHAR(36)         NOT NULL
            ,transactionid                  VARCHAR(36)         NOT NULL
            ,eventtime                      VARCHAR(30)
            ,direction                      VARCHAR(8)
            ,eventtype                      VARCHAR(10)
            ,creationdate                   VARCHAR(20)
            ,accountholdernationalid        VARCHAR(16)
            ,accountholderaccount           STRING
            ,counterpartynationalid         VARCHAR(16)
            ,counterpartyaccount            STRING
            ,tenantid                       VARCHAR(8)
            ,fromid                         VARCHAR(8)
            ,accountagentid                 VARCHAR(8)
            ,fromfibranchid                 VARCHAR(6)
            ,accountnumber                  VARCHAR(16)
            ,toid                           VARCHAR(8)
            ,accountidcode                  VARCHAR(5)
            ,counterpartyagentid            VARCHAR(8)
            ,tofibranchid                   VARCHAR(6)
            ,counterpartynumber             VARCHAR(16)
            ,counterpartyidcode             VARCHAR(5)
            ,amount                         STRING
            ,msgtype                        VARCHAR(6)
            ,settlementclearingsystemcode   VARCHAR(5)
            ,paymentclearingsystemreference VARCHAR(12)
            ,requestexecutiondate           VARCHAR(10)
            ,settlementdate                 VARCHAR(10)
            ,destinationcountry             VARCHAR(30)
            ,localinstrument                VARCHAR(2)
            ,msgstatus                      VARCHAR(12)
            ,paymentmethod                  VARCHAR(4)
            ,settlementmethod               VARCHAR(4)
            ,transactiontype                VARCHAR(2)
            ,verificationresult             VARCHAR(4)
            ,numberoftransactions           INT
            ,schemaversion                  INT
            ,usercode                       VARCHAR(4)
            ,created_at                     TIMESTAMP_LTZ(3)
            ,WATERMARK                      FOR created_at AS created_at - INTERVAL '15' SECOND
            ,PRIMARY KEY (_id) NOT ENFORCED
        ) WITH (
             'connector'                           = 'postgres-cdc'
            ,'hostname'                            = 'postgrescdc'
            ,'port'                                = '5432'
            ,'username'                            = 'dbadmin'
            ,'password'                            = 'dbpassword'
            ,'database-name'                       = 'demog'
            ,'schema-name'                         = 'public'
            ,'table-name'                          = 'transactions'
            ,'slot.name'                           = 'transactions0'
            -- experimental feature: incremental snapshot (default off)
            ,'scan.incremental.snapshot.enabled'   = 'true'               -- experimental feature: incremental snapshot (default off)
            ,'scan.startup.mode'                   = 'initial'            -- https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/flink-sources/postgres-cdc/#startup-reading-position     ,'decoding.plugin.name'                = 'pgoutput'
            ,'decoding.plugin.name'                = 'pgoutput'
        );
    """

    table_env.execute_sql(source_table_creation_sql)
    
    
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
    table_env.create_temporary_system_function("generate_txn_embedding", generate_txn_embedding)


    # --------------------------------------------------------------------------
    # Our Target table
    # Insert into pre-created sink table
    # --------------------------------------------------------------------------    
    # CREATE TABLE c_paimon.finflow.transactions_embedded (
    #      _id                            BIGINT              NOT NULL
    #     ,eventtime                      VARCHAR(30)
    #     ,direction                      VARCHAR(8)
    #     ,eventtype                      VARCHAR(10)
    #     ,creationdate                   VARCHAR(20)
    #     ,accountholdernationalid        VARCHAR(16)
    #     ,accountholderaccount           STRING
    #     ,counterpartynationalid         VARCHAR(16)
    #     ,counterpartyaccount            STRING
    #     ,tenantid                       VARCHAR(8)
    #     ,fromid                         VARCHAR(8)
    #     ,accountagentid                 VARCHAR(8)
    #     ,fromfibranchid                 VARCHAR(6)
    #     ,accountnumber                  VARCHAR(16)
    #     ,toid                           VARCHAR(8)
    #     ,accountidcode                  VARCHAR(5)
    #     ,counterpartyagentid            VARCHAR(8)
    #     ,tofibranchid                   VARCHAR(6)
    #     ,counterpartynumber             VARCHAR(16)
    #     ,counterpartyidcode             VARCHAR(5)
    #     ,amount                         STRING
    #     ,msgtype                        VARCHAR(6)
    #     ,settlementclearingsystemcode   VARCHAR(5)
    #     ,paymentclearingsystemreference VARCHAR(12)
    #     ,requestexecutiondate           VARCHAR(10)
    #     ,settlementdate                 VARCHAR(10)
    #     ,destinationcountry             VARCHAR(30)
    #     ,localinstrument                VARCHAR(2)
    #     ,msgstatus                      VARCHAR(12)
    #     ,paymentmethod                  VARCHAR(4)
    #     ,settlementmethod               VARCHAR(4)
    #     ,transactiontype                VARCHAR(2)
    #     ,verificationresult             VARCHAR(4)
    #     ,numberoftransactions           INT
    #     ,schemaversion                  INT
    #     ,usercode                       VARCHAR(4)
    #     -- New embedding vector column
    #     ,embedding_vector              ARRAY<FLOAT>
    #     ,embedding_dimensions          INT
    #     ,embedding_timestamp           TIMESTAMP_LTZ(3)  
    #     ,created_at                    TIMESTAMP_LTZ(3)
    #     ,PRIMARY KEY (_id) NOT ENFORCED
    # );


    embedding_insert_query = f"""
        INSERT INTO c_paimon.finflow.transactions_embedded
        SELECT 
             _id
            ,eventtime
            ,direction
            ,eventtype
            ,creationdate
            ,accountholdernationalid
            ,accountholderaccount
            ,counterpartynationalid
            ,counterpartyaccount
            ,tenantid
            ,fromid
            ,accountagentid
            ,fromfibranchid
            ,accountnumber
            ,toid
            ,accountidcode
            ,counterpartyagentid
            ,tofibranchid
            ,counterpartynumber
            ,counterpartyidcode
            ,amount
            ,msgtype
            ,settlementclearingsystemcode
            ,paymentclearingsystemreference
            ,requestexecutiondate
            ,settlementdate
            ,destinationcountry
            ,localinstrument
            ,msgstatus
            ,paymentmethod
            ,settlementmethod
            ,transactiontype
            ,verificationresult
            ,numberoftransactions
            ,schemaversion
            ,usercode
            ,generate_txn_embedding(
                eventtime
                ,direction
                ,eventtype
                ,creationdate
                ,accountholdernationalid
                ,accountholderaccount
                ,counterpartynationalid
                ,counterpartyaccount
                ,tenantid
                ,fromid
                ,accountagentid
                ,fromfibranchid
                ,accountnumber
                ,toid
                ,accountidcode
                ,counterpartyagentid
                ,tofibranchid
                ,counterpartynumber
                ,counterpartyidcode
                ,amount
                ,msgtype
                ,settlementclearingsystemcode
                ,paymentclearingsystemreference
                ,requestexecutiondate
                ,settlementdate
                ,destinationcountry
                ,localinstrument
                ,msgstatus
                ,paymentmethod
                ,settlementmethod
                ,transactiontype
                ,verificationresult
                ,numberoftransactions
                ,schemaversion
                ,usercode
            ) AS embedding_vector
            ${dimensions} AS embedding_dimensions
            ,CURRENT_TIMESTAMP(3) AS embedding_timestamp
            ,created_at
        FROM postgres_catalog.demog.transactions
    """

    statement_set.add_insert_sql(embedding_insert_query)
    print("Starting transactions embedding pipeline...")
    
    statement_set.execute().wait()
    print("Embedding pipeline completed successfully!")

#    table_env.execute_sql(embedding_insert_query).wait()


#end main

if __name__ == "__main__":
        
    try:
        main()
        
    except Exception as err:
        print(f"An unexpected error occurred: {err}")
        sys.exit(1)
        
    #end try
#end __main__