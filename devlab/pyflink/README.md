# Complete PyFlink UDF Setup for Flink 1.20.x

## Deploy

### Directory Structure

project/
├── scripts/
│   ├── README.md
│   ├── register_ah_udfs.sql
│   └── register_txn_udfs.sql
├── udfs/
│   ├── README.md
│   ├── txn_embed_udf.py
│   └── ah_embed_udf.py
├── master.sql
├── txn_embed.cmd
└── README.md


### Start SQL Client

```bash
make fsql
```


### At the prompt:

```sql

SOURCE /pyflink/scripts/register_ah_udfs.sql;

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

-- Register the function
CREATE TEMPORARY FUNCTION upper_case AS 'upper_udf.uppercase_str' LANGUAGE PYTHON;
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




#!/bin/bash

export PYFLINK_CLIENT_EXECUTABLE=/usr/bin/python3

SQL_CLIENT="/opt/flink/bin/sql-client.sh"

# Set Python path for UDFs
export PYTHONPATH="/pyflink/udfs:$PYTHONPATH"

# Execute with Python configuration
$SQL_CLIENT \
  -pyexec /usr/bin/python3 \
  -pyfs file:///pyflink/udfs \
  -f master.sql


# Complete Example Structure**
```
project/
│
├── scripts/
│   ├── README.md
│   ├── register_ah_embed_udfs.sql
│   ├── register_txn_embed_udfs.sql
│   └── test_ah_udf.sql
│
├── udfs/
│   ├── ah_embed_udf.py
│   ├── README.md
│   └── txn_embed_udf.py
│
├── master.sh
├── master.sql
├── README.md
├── README1.md
└── txn_embed.cmd
```

## NOTE: Tables are created via devlab/creFlinkFlows/master.sql also mounted onto Jobmanager into /creFlinkFlows/master.sql




## Deploying our Pyflink UDF's(User Defined Functions.)

### Deployment

I have designed the embedding "example" using two different patterns.

- The first is an `Insert into <target table> (<columns>) as select (<columns>) from source table;`
  
  For this option we include a call to the generate_ah_embedding() UDF as part of the select block, providing the function with the required field values.

  - This is accomplished by deploying the script as per `<Project root>/devlab/creFlinkFlows/scripts/4.1.creInsertsAh.sql`, You can either copy/past the commands contained in the sql script, or

  - xxx  or 
  
  - by executing `make ins_ah` from withing the `<Project root>/devlab/` directory.


- The second is a standalone Pyflink job executed at the `Jobmanager` CLI from within the `/pyflink/` directory.
  
  - `make jm`

  - `cd /pyflink`
  
  - copy command from `txn_embed.cmd` into jobmanager terminal and hit ENTER.
  
  - or by executing `make ins_txn` from withing the `<Project root>/devlab/` directory.




### Flattening our json structured "string" column 

This is accomplished using an awesome package written and made available by [Hans-Peter Grahsl](https://www.linkedin.com/in/hpgrahsl/) ...

We will consume our inboundPostgreSQL source tables, take the the original jsonb columns, that are now represented as `string` columns in our Flink CDC table and convert this back into a complex structured row or array of rows or unwind'd into a new table.

Git Repo: [Flink-json-udf](https://github.com/hpgrahsl/flink-json-udfs)

