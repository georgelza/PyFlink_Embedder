#######################################################################################################################
#
#   Project             :   Transaction... Streaming transaction embedding vectorizer 
#   File                :   txn_embed_udf.py
#   Created             :   8 Dec 2025
#   Description         :   Calculate vector values for record arriving in source table, outputting the columns + vector 
#                       :   to a new target table
#
#   Usage in Jobmanager:
#   /opt/flink/bin/flink run \
#        -d \
#        -m jobmanager:8081 \
#        -py /pyflink/udfs/txn_embed_udf.py \
#        -j /opt/flink/lib/flink-sql-connector-postgres-cdc-3.5.0.jar \
#        -j /opt/flink/lib/flink-python-1.20.1.jar
#
########################################################################################################################
__author__      = "George Leonard"
__email__       = "georgelza@gmail.com"
__version__     = "1.0.1"
__copyright__   = "Copyright 2025 - G Leonard"

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.udf import udf
from pyflink.table import DataTypes
from pyflink.common import Configuration
from sentence_transformers import SentenceTransformer
from datetime import datetime
import torch
import sys


# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------
DIMENSIONS      = 384
MODEL           = 'sentence-transformers/all-MiniLM-L6-v2'


# --------------------------------------------------------------------------
# Define UDF for embedding generation
# --------------------------------------------------------------------------
@udf(result_type=DataTypes.ARRAY(DataTypes.DOUBLE()))
def generate_txn_embedding(target_dimensions, eventtime, direction, eventtype, creationdate,
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
        Generate <target_dimensions>-dimensional embeddings for financial transactions using sentence-transformers.
        This UDF lazy-loads the model to avoid serialization issues.

        Args:
            embedding dimensions,
            transaction details,
            
        Returns:
            array of float: ....
    """
    
    try:
        # Use a class variable to cache the model across invocations
        if not hasattr(generate_txn_embedding, 'model'):
            generate_txn_embedding.model = SentenceTransformer(MODEL)
            generate_txn_embedding.model.eval()
        
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
            transaction_text = "Unknown transaction profile"
        
        # Generate embedding
        with torch.no_grad():
            embedding = generate_txn_embedding.model.encode(transaction_text, convert_to_numpy=True)
            #print(embedding)

        final_size = int(target_dimensions)
                                
        # Convert to list of floats for Flink
        return embedding[:final_size].astype('float64').tolist()
    
    except Exception as e:
        print(f"UDF Error: {str(e)}", file=sys.stderr)
        raise e

# end generate_embedding


def main():
    """
    Main function to set up and execute the Flink streaming job for transaction embedding generation.
    """
    pipeline_name   = "Flink_txn_embedding"
    now             = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"Starting {pipeline_name} - {now}")

    target_dimensions = DIMENSIONS
    
    # --------------------------------------------------------------------------
    # Environment settings
    # --------------------------------------------------------------------------
    config = Configuration()
    config.set_string("table.exec.source.idle-timeout", "1s")
    config.set_string("pipeline.name", pipeline_name)
    
    # CRITICAL: Set execution mode to not wait for job completion
    config.set_string("execution.attached", "false")
    config.set_string("execution.shutdown-on-attached-exit", "false")

    # Initialize PyFlink environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    env_settings = EnvironmentSettings \
        .new_instance() \
        .in_streaming_mode() \
        .with_configuration(config) \
        .build()

    table_env = StreamTableEnvironment.create(env, environment_settings=env_settings)

    # --------------------------------------------------------------------------
    # Add JAR dependencies
    # --------------------------------------------------------------------------
    postgres_cdc_jar  = "file:///opt/flink/lib/flink-sql-connector-postgres-cdc-3.5.0.jar" 
    flink_python_jar  = "file:///opt/flink/lib/flink-python-1.20.2.jar"
    postgres_jdbc_jar = "file:///opt/flink/lib/postgresql-42.7.6.jar"
    
    table_env.get_config().set("pipeline.jars", f"{postgres_cdc_jar};{flink_python_jar};{postgres_jdbc_jar}")

    # --------------------------------------------------------------------------
    # Register Postgres CDC Catalog
    # --------------------------------------------------------------------------
    print("Creating catalog c_cdcsource...")
    table_env.execute_sql("CREATE CATALOG c_cdcsource WITH ('type'='generic_in_memory');")
    
    print("Creating database c_cdcsource.demog...")
    table_env.execute_sql("CREATE DATABASE IF NOT EXISTS c_cdcsource.demog;")

    # --------------------------------------------------------------------------
    # Create the source table with LATEST-OFFSET startup mode
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
    print("✓ Source table created successfully.")
    
    # --------------------------------------------------------------------------
    # Register Apache Paimon Catalog
    # --------------------------------------------------------------------------
    print("Creating Paimon catalog c_paimon...")
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
    table_env.execute_sql("CREATE DATABASE IF NOT EXISTS c_paimon.finflow;")
    print("✓ Paimon catalog and database ready.")

    # --------------------------------------------------------------------------
    # Register the UDF
    # --------------------------------------------------------------------------    
    print("Registering UDF generate_txn_embedding...")
    table_env.create_temporary_system_function("generate_txn_embedding", generate_txn_embedding)
    print("✓ UDF registered successfully.")

    # --------------------------------------------------------------------------
    # Create statement set and add INSERT query
    # --------------------------------------------------------------------------
    statement_set = table_env.create_statement_set()

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
                 {target_dimensions}
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
            ,{target_dimensions} AS embedding_dimensions
            ,CURRENT_TIMESTAMP   AS embedding_timestamp
            ,created_at
        FROM c_cdcsource.demog.transactions
    """

    statement_set.add_insert_sql(embedding_insert_query)
    
    # --------------------------------------------------------------------------
    # Execute the job in detached mode
    # --------------------------------------------------------------------------
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")    
    print(f"\nSubmitting job to Flink cluster... - {now}")
    print("="*80)
    
    try:
        # Execute returns immediately in detached mode
        table_result = statement_set.execute()
        
        # Try to get job client info if available
        try:
            job_client = table_result.get_job_client()
            if job_client:
                job_id = job_client.get_job_id()
                print(f"✓ Job submitted successfully!")
                print(f"✓ Job ID: {job_id}")
            else:
                print(f"✓ Job submitted successfully (detached mode)!")
        except:
            print(f"✓ Job submitted successfully (detached mode)!")
        
        print(f"✓ Pipeline: {pipeline_name}")
        print(f"✓ Check job status: http://localhost:8084")
        print(f"✓ The job will process CDC events continuously")
        print("="*80)
        
    except Exception as e:
        print(f"✗ Error during job submission: {e}")
        import traceback
        traceback.print_exc()
        raise
    
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")    
    print(f"\nScript completed - {now}")
    print("Job is now running in the cluster. This script will exit.")
    print("="*80)

#end main


if __name__ == "__main__":
    
    print("="*80)
    print("STARTING TXN_EMBED_UDF.PY")
    print("="*80)
    
    try:
        main()
        print("\n" + "="*80)
        print("MAIN() COMPLETED SUCCESSFULLY")
        print("Check Flink UI for job status: http://localhost:8084")
        print("="*80)
        sys.exit(0)
        
    except Exception as err:
        print("\n" + "="*80)
        print(f"FATAL ERROR IN MAIN(): {err}")
        print("="*80)
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
# end "__main__"