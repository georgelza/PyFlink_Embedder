-- Generated master.sql

-- Thinking is we will consume from PostgreSQL sources, then use a PyFlink to flatten the structure, outputting to a Fluss table.
-- then reading new Fluss table, calculate embeddings, output to another Fluss table/s. 
-- This in turn will be configured with Lakehouse tiering into Paimon table on local FS into /data directory 

-- Catalog c_cdcsource moved to 2.1.creCdc.sql
-- This is due t this being an generic in memory catalog and is required to be re-created in every session together with the 
-- CDC backed tables.

USE CATALOG default_catalog;
  
CREATE CATALOG c_cdcsource WITH 
    ('type'='generic_in_memory'); 

USE CATALOG c_cdcsource;

-- Source for PyFlink
CREATE DATABASE IF NOT EXISTS demog;            

-- Paimon based Catalog stored inside PostgreSQL database using JDBC interface
-------------------------------------------------------------------------------------------------------------------------
-- server: postgrescat
-- db:     flink_catalog
-- schema: paimon_catalog
CREATE CATALOG c_paimon WITH (
     'type'                          = 'paimon'
    ,'metastore'                     = 'jdbc'                      
    ,'catalog-key'                   = 'jdbc'
    ,'uri'                           = 'jdbc:postgresql://postgrescat:5432/flink_catalog?currentSchema=paimon_catalog'
    ,'jdbc.user'                     = 'dbadmin'
    ,'jdbc.password'                 = 'dbpassword'
    ,'jdbc.driver'                   = 'org.postgresql.Driver'
    ,'warehouse'                     = 'file:///data'
    ,'table-default.file.format'     = 'parquet'
);

USE CATALOG c_paimon;

-- Output from PyFlink routine, embedded tables
CREATE DATABASE IF NOT EXISTS c_paimon.finflow;

-- Output from CTAS - Flat structured, source from c_paimon.finflow, potentially to a different destination
CREATE DATABASE IF NOT EXISTS c_paimon.ctas;            

-- Transformed into complex JSON Structure, sourced from c_paimon.finflow
CREATE DATABASE IF NOT EXISTS c_paimon.cmplx;            

SHOW DATABASES;

-- next execute 2.1
-- scripts/3.1.creTargetFinflow.sql
-- Output - Tables for our embedding process, source from CDC tables
--

USE CATALOG c_paimon;

USE finflow;



CREATE OR REPLACE TABLE accountholders (
     _id                           BIGINT
    ,nationalid                    VARCHAR(16)
    ,firstname                     VARCHAR(100)
    ,lastname                      VARCHAR(100)
    ,dob                           VARCHAR(10)
    ,gender                        VARCHAR(10)
    ,children                      INT
    ,address                       STRING
    ,accounts                      STRING
    ,emailaddress                  VARCHAR(100)
    ,mobilephonenumber             VARCHAR(20)
    ,embedding_vector              ARRAY<DOUBLE>
    ,embedding_dimensions          INT
    ,embedding_timestamp           TIMESTAMP_LTZ(3)    
    ,created_at                    TIMESTAMP_LTZ(3)
    ,PRIMARY KEY (_id) NOT ENFORCED
);


CREATE OR REPLACE TABLE transactions (
     _id                            BIGINT              NOT NULL
    ,eventid                        VARCHAR(36)
    ,transactionid                  VARCHAR(36)
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
    ,embedding_vector               ARRAY<DOUBLE>
    ,embedding_dimensions           INT
    ,embedding_timestamp            TIMESTAMP_LTZ(3)  
    ,created_at                     TIMESTAMP_LTZ(3)
    ,PRIMARY KEY (_id) NOT ENFORCED
);

-- now see 4.1
-- scripts/3.2.creTargetCmplx.sql
-- Output - To be complex version of our embedded flat structured record
--  

USE CATALOG c_paimon;

USE cmplx;



CREATE OR REPLACE TABLE accountholders (
     _id                            VARCHAR(36)         NOT NULL                                -- UUIDV7 generated by app, inside 'data' / json payload
    ,nationalid                     VARCHAR(16)         NOT NULL
    ,firstName                      VARCHAR(100)
    ,lastName                       VARCHAR(100)
    ,dob                            VARCHAR(10)
    ,gender                         VARCHAR(10)
    ,children                       INT
    ,address                        ROW<
         streetAddress                  VARCHAR(20)
        ,suburb                         VARCHAR(200)
        ,town                           VARCHAR(200)
        ,provice                        VARCHAR(100)
        ,country                        VARCHAR(100)
        ,postalCode                     VARCHAR(20)
    >
    ,accounts                       ARRAY<ROW<
         tenantId                       VARCHAR(8)
        ,branchId                       VARCHAR(6)
        ,accountId                      VARCHAR(16)
        ,entityId                       VARCHAR(6)
        ,accountType                    VARCHAR(14)
        ,openingDate                    VARCHAR(12)
        ,idCode                         VARCHAR(5)
    >>
    ,eMailAddress                   VARCHAR(100)
    ,mobilePhoneNumber              VARCHAR(20)
    ,embedding_vector               ARRAY<DOUBLE>
    ,embedding_dimensions           INT
    ,embedding_timestamp            TIMESTAMP_LTZ(3)    
    ,created_at                     TIMESTAMP_LTZ(3)
    ,WATERMARK FOR created_at AS created_at - INTERVAL '15' SECOND
    ,PRIMARY KEY (nationalid) NOT ENFORCED
) WITH (
     'file.format'                       = 'parquet'
    ,'compaction.min.file-num'           = '2'
    ,'compaction.early-max.file-num'     = '50'
    ,'snapshot.time-retained'            = '1h'
    ,'snapshot.num-retained.min'         = '5'
    ,'snapshot.num-retained.max'         = '20'
    ,'table.exec.sink.upsert-materialize'= 'NONE'
);

CREATE OR REPLACE TABLE transactions (
     _id                            VARCHAR(10)     NOT NULL                         -- UUIDV7 generated by app, inside 'data' / json payload
    ,eventid                        VARCHAR(36)     NOT NULL
    ,transactionid                  VARCHAR(36)     NOT NULL
    ,eventtime                      VARCHAR(30)
    ,direction                      VARCHAR(8)
    ,eventtype                      VARCHAR(10)
    ,creationDate                   VARCHAR(20)
    ,accountholdernationalId        VARCHAR(16)
    ,accountholderaccount           ROW<
        ,tenantId                       VARCHAR(8)
        ,branchId                       VARCHAR(6)
        ,accountId                      VARCHAR(16)
        ,entityId                       VARCHAR(8)
        ,accountType                    VARCHAR(12)
        ,openingDate                    VARCHAR(12)
        ,idCode                         VARCHAR(5)
    >
    ,counterpartynationalId         VARCHAR(16)
    ,counterpartyaccount            ROW<
        ,tenantId                       VARCHAR(8)
        ,branchId                       VARCHAR(6)
        ,accountId                      VARCHAR(16)
        ,entityId                       VARCHAR(8)
        ,accountType                    VARCHAR(12)
        ,openingDate                    VARCHAR(12)
        ,idCode                         VARCHAR(5)
    >
    ,tenantId                       VARCHAR(8)
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
    ,amounts                        ROW<
         baseCurrency                   VARCHAR(4)
        ,baseValue                      VARCHAR(10)
        ,roe                            VARCHAR(10)
        ,currency                       VARCHAR(4)
        ,`value`                        VARCHAR(10)
    >
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
    ,numberoftransactions           INTEGER
    ,schemaversion                  INTEGER
    ,usercode                       VARCHAR(4)
    ,embedding_vector               ARRAY<DOUBLE>
    ,embedding_dimensions           INT
    ,embedding_timestamp            TIMESTAMP_LTZ(3)    
    ,created_at                     TIMESTAMP_LTZ(3)
    ,WATERMARK FOR created_at AS created_at - INTERVAL '15' SECOND
    ,PRIMARY KEY (eventid) NOT ENFORCED
) WITH (
     'file.format'                       = 'parquet'
    ,'compaction.min.file-num'           = '2'
    ,'compaction.early-max.file-num'     = '50'
    ,'snapshot.time-retained'            = '1h'
    ,'snapshot.num-retained.min'         = '5'
    ,'snapshot.num-retained.max'         = '20'
    ,'table.exec.sink.upsert-materialize'= 'NONE'
);

-- See 4.2




