
# CDC Sources
CREATE OR REPLACE TABLE c_cdcsource.demog.accountholders (
     _id                BIGINT                  NOT NULL
    ,nationalid         VARCHAR(16)             NOT NULL
    ,firstname          VARCHAR(100)
    ,lastname           VARCHAR(100)
    ,dob                VARCHAR(10) 
    ,gender             VARCHAR(10)
    ,children           INT
    ,address            STRING
    ,accounts           STRING
    ,emailaddress       VARCHAR(100)
    ,mobilephonenumber  VARCHAR(20)
    ,created_at         TIMESTAMP_LTZ(3)
    ,WATERMARK          FOR created_at AS created_at - INTERVAL '15' SECOND
    ,PRIMARY KEY (_id) NOT ENFORCED
) WITH (
     'connector'                           = 'postgres-cdc'
    ,'hostname'                            = 'postgrescdc'
    ,'port'                                = '5432'
    ,'username'                            = 'dbadmin'
    ,'password'                            = 'dbpassword'
    ,'database-name'                       = 'demog'
    ,'schema-name'                         = 'public'
    ,'table-name'                          = 'accountholders'
    ,'slot.name'                           = 'accountholders0'
    -- experimental feature: incremental snapshot (default off)
    ,'scan.incremental.snapshot.enabled'   = 'true'               -- experimental feature: incremental snapshot (default off)
    ,'scan.startup.mode'                   = 'initial'            -- https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/flink-sources/postgres-cdc/#startup-reading-position     ,'decoding.plugin.name'                = 'pgoutput'
    ,'decoding.plugin.name'                = 'pgoutput'
);


CREATE OR REPLACE TABLE c_cdcsource.demog.transactions (
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
    ,'slot.name'                           = 'transactions0'
    -- experimental feature: incremental snapshot (default off)
    ,'scan.incremental.snapshot.enabled'   = 'true'               -- experimental feature: incremental snapshot (default off)
    ,'scan.startup.mode'                   = 'initial'            -- https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/flink-sources/postgres-cdc/#startup-reading-position     ,'decoding.plugin.name'                = 'pgoutput'
    ,'decoding.plugin.name'                = 'pgoutput'
);

################################################################################################################################################

-- now see 3.1.creTarget.sql
