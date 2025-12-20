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
#   in Jobmanager i.e.
#
#   /opt/flink/bin/flink run \
#        -m jobmanager:8081 \
#        -py /pyflink/udfs/txn_embed_udf.py \
#        -j /opt/flink/lib/flink-sql-connector-postgres-cdc-3.5.0.jar \
#        -j /opt/flink/lib/flink-python-1.20.1.jar
#
########################################################################################################################
__author__      = "George Leonard"
__email__       = "georgelza@gmail.com"
__version__     = "1.0.0"
__copyright__   = "Copyright 2025 - G Leonard"

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.udf import udf
from pyflink.table import DataTypes
from pyflink.common import Configuration
from sentence_transformers import SentenceTransformer
from datetime import datetime
import sys
import torch


# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------
DIMENSIONS      = 384
MODEL           = 'sentence-transformers/all-MiniLM-L6-v2'
# sentence-transformers/all-mpnet-base-v2   (768D) - Higher quality slower
# BAAI/bge-small-en-v1.5                    (384D) - Optimized for retrieval tasks
# intfloat/e5-small-v2                      (384D) - Good for semantic similarity


# --------------------------------------------------------------------------
# Define UDF for embedding generation
# --------------------------------------------------------------------------
@udf(result_type=DataTypes.ARRAY(DataTypes.FLOAT()))
def generate_txn_embedding(dimensions, eventtime, direction, eventtype, creationdate,
                           accountholdernationalid, accountholderaccount,
                           counterpartynationalid, counterpartyaccount,
                           tenantid, fromid, accountagentid, fromfibranchid,
                           accountnumber, toid, accountidcode, counterpartyagentid,
                           tofibranchid, counterpartynumber, counterpartyidcode,
                           amount, msgtype, settlementclearingsystemcode,
                           paymentclearingsystemreference, requestexecutiondate,
                           settlementdate, destinationcountry, localinstrument,
                           msgstatus, paymentmethod, settlementmethod,
                           transactiontype, verificationresult, numberoftransactions,
                           schemaversion, usercode):
    """
    Generate 384-dimensional embeddings for financial transactions using sentence-transformers.
    This UDF lazy-loads the model to avoid serialization issues.
    
    Excludes fields: eventid, transactionid, embedding_timestamp and embedding_dimensions, created_at from embedding generation
    """
    
    # Use a class variable to cache the model across invocations
    if not hasattr(generate_txn_embedding, 'model'):
        generate_txn_embedding.model = SentenceTransformer(MODEL)
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

    # Ensure correct dimensions
    embedding = embedding[:dimensions]
    
    # Convert to list of floats for Flink
    return embedding.tolist()

# end generate_txn_embedding


def main():
    """
    Main function to set up and execute the Flink streaming job for transaction embedding generation.
    """
    pipeline_name = "Flink_txn_embedding"
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Starting {pipeline_name} - {now}")

    # --------------------------------------------------------------------------
    # Some environment settings and a nice Jobname/description "pipeline.name" for flink console
    # --------------------------------------------------------------------------
    config = Configuration()
    config.set_string("table.exec.source.idle-timeout", "1s")
    config.set_string("pipeline.name", pipeline_name)

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
    # --------------------------------------------------------------------------
    postgres_cdc_jar = "file:///opt/flink/lib/flink-sql-connector-postgres-cdc-3.5.0.jar" 
    flink_python_jar = "file:///opt/flink/lib/flink-python-1.20.2.jar"
    postgres_jdbc_jar = "file:///opt/flink/lib/postgresql-42.7.6.jar"
    
    table_env.get_config().set("pipeline.jars", f"{postgres_cdc_jar};{flink_python_jar};{postgres_jdbc_jar}")

    # --------------------------------------------------------------------------
    # Register Postgres Input Catalog
    # --------------------------------------------------------------------------
    print("Creating catalog c_cdcsource...")
    table_env.execute_sql("""
        CREATE CATALOG c_cdcsource WITH 
        ('type'='generic_in_memory'); 
    """)
    
    # Create the database in the catalog
    print("Creating database c_cdcsource.demog...")
    table_env.execute_sql("""
        CREATE DATABASE IF NOT EXISTS c_cdcsource.demog;
    """)

    # --------------------------------------------------------------------------
    # Create the source table
    # --------------------------------------------------------------------------
    print("Creating source table c_cdcsource.demog.transactions...")
    source_table_creation_sql = """
        CREATE TABLE c_cdcsource.demog.transactions (
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
            ,'slot.name'                           = 'transactions_python_udf'
            ,'scan.incremental.snapshot.enabled'   = 'true'
            ,'scan.startup.mode'                   = 'initial'
            ,'decoding.plugin.name'                = 'pgoutput'
            ,'debezium.snapshot.mode'              = 'initial'
        );
    """

    table_env.execute_sql(source_table_creation_sql)
    print("Source table created successfully.")
    
    # --------------------------------------------------------------------------
    # Register Apache Paimon output Catalog
    # Using JDBC metastore to match your production setup (1.1.creCat.sql)
    # --------------------------------------------------------------------------
    print("Creating Paimon catalog c_paimon (JDBC metastore)...")
    table_env.execute_sql("""
        CREATE CATALOG c_paimon WITH (
            'type'                          = 'paimon'
            ,'metastore'                    = 'jdbc'
            ,'catalog-key'                  = 'jdbc'
            ,'uri'                          = 'jdbc:postgresql://postgrescat:5432/flink_catalog?currentSchema=paimon_catalog'
            ,'jdbc.user'                    = 'dbadmin'
            ,'jdbc.password'                = 'dbpassword'
            ,'jdbc.driver'                  = 'org.postgresql.Driver'
            ,'warehouse'                    = 's3://warehouse/paimon'
            ,'s3.endpoint'                  = 'http://minio:9000'
            ,'s3.path-style-access'         = 'true'
            ,'table-default.file.format'    = 'parquet'
        );
    """)
    
    print("Creating database c_paimon.finflow...")
    table_env.execute_sql("""
        CREATE DATABASE IF NOT EXISTS c_paimon.finflow;
    """)

    # --------------------------------------------------------------------------
    # Register the UDF (must be done AFTER the function is defined)
    # --------------------------------------------------------------------------    
    print("Registering UDF generate_txn_embedding...")
    table_env.create_temporary_system_function("generate_txn_embedding", generate_txn_embedding)

    # --------------------------------------------------------------------------
    # Create statement set for managing multiple insert statements
    # --------------------------------------------------------------------------
    statement_set = table_env.create_statement_set()

    # --------------------------------------------------------------------------
    # Our Target table - Insert into pre-created sink table
    # --------------------------------------------------------------------------    
    # Target table structure from 3.1.creTargetFinflow.sql:
    # CREATE OR REPLACE TABLE transactions (
    #      _id                            BIGINT              NOT NULL
    #     ,eventid                        VARCHAR(36)
    #     ,transactionid                  VARCHAR(36)
    #     ,eventtime                      VARCHAR(30)
    #     ... (all transaction fields)
    #     ,embedding_vector              ARRAY<FLOAT>
    #     ,embedding_dimensions          INT
    #     ,embedding_timestamp           TIMESTAMP_LTZ(3)  
    #     ,created_at                    TIMESTAMP_LTZ(3)
    #     ,PRIMARY KEY (_id) NOT ENFORCED
    # );

    print("Preparing embedding insert query...")
    embedding_insert_query = f"""
        INSERT INTO c_paimon.finflow.transactions
        SELECT 
             _id
            ,eventid
            ,transactionid
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
                 {DIMENSIONS}
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
            ) AS embedding_vector
            ,{DIMENSIONS} AS embedding_dimensions
            ,CURRENT_TIMESTAMP AS embedding_timestamp
            ,created_at
        FROM c_cdcsource.demog.transactions
    """

    statement_set.add_insert_sql(embedding_insert_query)
    
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")    
    print(f"Submitting transactions embedding pipeline... - {now}")
    
    # Execute and get the job execution result
    # For streaming jobs submitted via 'flink run', we don't want to block
    # The job will continue running in the cluster after the script exits
    job_client = statement_set.execute()
    
    print(f"Job submitted successfully!")
    print(f"Job ID:              {job_client.get_job_id()}")
    print(f"Check job status at: http://localhost:8084")
    print(f"The streaming job will continue running in the Flink cluster.")
    
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")    
    print(f"Script completed - {now}")
    
    # Note: We don't call .wait() because that would block until the streaming job completes
    # Streaming jobs run indefinitely, so we just submit and exit

# end main


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        print(f"An unexpected error occurred: {err}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
# end __main__