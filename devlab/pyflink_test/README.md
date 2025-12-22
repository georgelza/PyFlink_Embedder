# Complete PyFlink UDF Setup for Flink 1.20.x

## Deploy

### Directory Structure

project/
├── scripts/
│   ├── queries_with_udfs.sql
│   ├── register_udfs.sql
│   ├── run_all.sql
│   └── test_udf.sql
├── udfs/
│   └── upper_udf.py
└── README.md


### Start SQL Client

```bash
/opt/flink/bin/sql-client.sh
```


### At the prompt:

```sql
SOURCE /pyflink/scripts/register_udfs.sql;
SOURCE /pyflink/scripts/queries_with_udfs.sql;
-- or 
SOURCE /pyflink/scripts/run_all.sql;
```


### File 1: udfs/upper_udf.py

```python
#######################################################################################################################
#
#   Project             :   PyFlink UDF Lab
#   File                :   upper_udf.py
#   Description         :   Scalar UDF to uppercase a string.
#
########################################################################################################################
__author__      = "George Leonard"
__email__       = "georgelza@gmail.com"
__version__     = "1.0.0"
__copyright__   = "Copyright 2025, - G Leonard"

from pyflink.table import DataTypes
from pyflink.table.udf import udf

# Define the scalar UDF using the @udf decorator
@udf(result_type=DataTypes.STRING())
def uppercase_str(input_string: str) -> str:
    """
    Converts input string to uppercase.
    
    Args:
        input_string: String to convert
        
    Returns:
        Uppercase version of input string, or None if input is None
    """
    if input_string is None:
        return None
    return input_string.upper()
```


### File 2: scripts/register_udfs.sql

```sql
-- scripts/register_udfs.sql
-- Register Python UDF for use in Flink SQL Client
-- Flink Version: 1.20.x

-- Add Python file to the session
ADD FILE '/absolute/path/to/udfs/upper_udf.py';

-- Register the UDF
-- Syntax: CREATE FUNCTION <function_name> AS '<module_name>.<function_name>' LANGUAGE PYTHON;
CREATE TEMPORARY FUNCTION upper_case AS 'upper_udf.uppercase_str' LANGUAGE PYTHON;

-- Verify registration
SHOW FUNCTIONS;
```


### File 3: scripts/queries_with_udfs.sql

```sql
-- scripts/queries_with_udfs.sql
-- Execute INSERT query using the registered UDF

INSERT INTO sink_table (id, upper_name)
SELECT 
    id,
    upper_case(name) as upper_name
FROM source_table;
```


### File 4: scripts/test_udf.sql (Optional - for testing)

```sql
-- scripts/test_udf.sql
-- Test the UDF before running the full INSERT

-- Test with inline values
SELECT upper_case('hello world') as result;

-- Test with source table (preview)
SELECT 
     id
    ,firstname,
    ,lastname,
    ,upper_case(firstname) as upper_firstname
    ,upper_case(lastname)  as upper_lastname
FROM c_cdcsource;
```

```bash
# Execution Instructions
# Step 1: Update File Paths
# Edit scripts/register_udfs.sql and replace /absolute/path/to/udfs/upper_udf.py with the actual absolute path:

# Find your absolute path
pwd
```

### Example: /pyflink

```sql
-- Update line in register_udfs.sql:
ADD FILE '/pyflink/udfs/upper_udf.py';
```

```bash
# Step 2: Start Flink SQL Client
cd $FLINK_HOME
bin/sql-client.sh
```

```sql
-- Step 3: Register the UDF
-- At the SQL Client prompt:
-- Execute the registration script
SOURCE /pyflink/scripts/register_udfs.sql;

-- You should see output confirming the function is registered
-- Check with:
SHOW FUNCTIONS;

-- Step 4: Test the UDF (Optional but Recommended)
-- Quick test
SELECT upper_case('test') as result;

-- Should return: TEST
-- Step 5: Execute the INSERT Query
-- Run your insert script
SOURCE /pyflink/scripts/queries_with_udfs.sql;

-- Troubleshooting
--- Issue 1: "Could not find function 'upper_case'"
--- Solution: Make sure you executed register_udfs.sql first. Check with SHOW FUNCTIONS;

--- Issue 2: "ModuleNotFoundError: No module named 'upper_udf'"
--- Solution: The ADD FILE path must be absolute. Update the path in register_udfs.sql.

--- Issue 3: "Python UDF not supported"
--- Solution: Ensure Python 3.7+ is installed and accessible:
```

```bash 
python3 --version
which python3

# Set PYFLINK_PYTHON environment variable if needed:
export PYFLINK_PYTHON=python3
```

```sql
-- Issue 4: Tables don't exist
-- Make sure your source and sink tables are created before running the INSERT:
-- Verify tables exist
SHOW TABLES;

-- Check table structure
DESCRIBE c_cdcsource.demog.accountholders;
DESCRIBE c_paimon.finflow.accountholders;

-- Alternative: Run Everything in One Session
-- You can also combine everything in a single SQL file:
-- File: scripts/run_all.sql
-- Add Python UDF file
ADD FILE '/pyflink/udfs/upper_udf.py';

-- Register the function
CREATE TEMPORARY FUNCTION upper_case AS 'upper_udf.uppercase_str' LANGUAGE PYTHON;

-- Test it
SELECT upper_case('hello') as test;

-- Run the INSERT
SELECT 
     id
    ,firstname,
    ,lastname,
    ,upper_case(firstname) as upper_firstname
    ,upper_case(lastname)  as upper_lastname
FROM c_cdcsource
```

```bash
# Execute with:
/opt/flink/bin/sql-client.sh -f /pyflink/scripts/run_all.sql

# Environment Variables (Optional)
# Add to your .bashrc or .zshrc:
# Flink environment
export FLINK_HOME=/opt/flink
export PATH=$FLINK_HOME/bin:$PATH


## # Python configuration for PyFlink
export PYFLINK_PYTHON=python3


### Project paths (optional convenience)
export FLINK_UDF_HOME=/pyflink
```

### Notes for Flink 1.20.x

Python UDFs are fully supported in Flink 1.20.x

- ADD FILE is the correct way to add Python files to the session
- CREATE TEMPORARY FUNCTION with LANGUAGE PYTHON is the standard syntax
- UDFs registered with CREATE TEMPORARY FUNCTION only exist for the current session
- For permanent UDFs, use CREATE FUNCTION (without TEMPORARY)

### Quick Reference

Command Purpose 

- ADD FILE '/path/to/file.py' Add Python file to session 
- CREATE TEMPORARY FUNCTION name AS 'module.func' 
- LANGUAGE PYTHON Register Python UDF 
- SHOW FUNCTIONS List all available functions 
- DROP FUNCTION name Remove a function 
- SOURCE /path/to/script.sql Execute SQL script file


## Summary

✅ UDF is defined in udfs/upper_udf.py
✅ Registration script is scripts/register_udfs.sql
✅ INSERT query is in scripts/queries_with_udfs.sql
✅ At SQL Client prompt: SOURCE register_udfs.sql then SOURCE queries_with_udfs.sql
✅ All files follow Flink 1.20.x syntax and conventions