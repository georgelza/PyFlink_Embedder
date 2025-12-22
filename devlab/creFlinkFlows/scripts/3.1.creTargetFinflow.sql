-- scripts/3.1.creTargetFinflow.sql
-- Outbound
-- 
-- Output Tables for our embedding process, source from CDC tables

-- See 2.1
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
