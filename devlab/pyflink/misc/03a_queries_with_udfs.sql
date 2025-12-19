-- scripts/03_queries_with_udfs.sql

CREATE TABLE sink_table AS
SELECT 
    id,
    upper_case(name) as upper_name,
    calculate_score(value1, value2) as score
FROM source_table;