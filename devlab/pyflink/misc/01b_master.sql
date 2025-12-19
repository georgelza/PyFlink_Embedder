-- Configuration
SOURCE '/project/scripts/01_config.sql';

-- Register PyFlink UDFs
SOURCE '/project/scripts/02_register_udfs.sql';

-- Create tables
SOURCE '/project/scripts/03_create_tables.sql';

-- Execute queries using UDFs
SOURCE '/project/scripts/04_queries_with_udfs.sql';


-- Important Considerations

-- Python environment must be configured before SQL Client starts
-- Use absolute paths for Python files in CREATE FUNCTION statements
-- LANGUAGE PYTHON is required in the CREATE FUNCTION statement
-- Dependencies should be packaged as ZIP and added via ADD JAR
-- Python version must match between client and cluster

-- Alternative: Pure Python Script
-- If you have complex UDF logic, consider using pure PyFlink: