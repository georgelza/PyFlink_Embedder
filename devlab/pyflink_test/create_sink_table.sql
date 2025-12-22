-- scripts/create_sink_table.sql
-- Create sink table for storing uppercase transformed names
-- Flink Version: 1.20.x

-- ============================================================================
-- Drop table if exists (optional - use with caution in production)
-- ============================================================================
-- DROP TABLE IF EXISTS sink_table;

-- ============================================================================
-- Create sink_table
-- ============================================================================
CREATE TABLE c_cdcsource.demog.sink_table (
    id              INT,
    upper_name      STRING,
    lastname        STRING,
    upper_firstname STRING,
    upper_lastname  STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'filesystem',
    'path'      = '/pyflink/sink_table',
    'format'    = 'csv',
    'csv.field-delimiter' = ',',
    'csv.ignore-parse-errors' = 'true'
);