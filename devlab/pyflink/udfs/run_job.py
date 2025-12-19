# run_job.py
from pyflink.table import EnvironmentSettings, TableEnvironment

env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

# Register UDF
from devlab.pyflink.udf.my_functions import upper_case
t_env.create_temporary_function("upper_case", upper_case)

# Execute SQL files
with open('scripts/create_tables.sql') as f:
    for statement in f.read().split(';'):
        if statement.strip():
            t_env.execute_sql(statement)
            
            
# The SQL-based approach (Method 1) integrates best with your packaging strategy and keeps everything in SQL scripts that can be orchestrated via your master.sql file.