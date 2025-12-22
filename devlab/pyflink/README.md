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
│   ├── txn_embed_udf.sql
│   └── ah_embed_udf.py
├── master.sh
├── master.sql
├── txn_embed.cmd
└── README.md


### Start SQL Client

```bash
/opt/flink/bin/sql-client.sh
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