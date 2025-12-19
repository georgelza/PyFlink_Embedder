## Vector Embedding of Account Holders & Financial Transactions in the Realtime Transaction world, at volume


1. Generate all data Using Shadowtraffic => into PostgreSQL

2. Consume from PostgreSQL => Apacke Flink tables using CDC

3. Flatten using PyFlink Function 1 -> New Tables

4. Calculate vector embedding values using PyFlink, comsuming New tables from #3. -> Function 2 (AccountHolders) and Function 3 (Transactions) => Output to new Flink Table

5. -> Push to Apache Paimon Tabes


BLOG: [Using Pyflink UDF to calculate embedding vectors on inbound tables via Flink CDC]()

GIT REPO: [PyFlink_Embedder](https://github.com/georgelza/PyFlink_Embedder.git)


## Deployment

- `<Project Root>/devlab/docker-compose.yml` which can be brought online by executing below, (this will use `.env`).

- Execute `make run_flink` as defined in `devlab/Makefile` to run environment.
  
- Execute `Shadowtraffic` to create Workload (#1 AccountHolders, #2 Financial Transactions) 
  - => Output to 2 PostgreSQL Tables located in postgrescdc Postgres based database/service.
  - This is done by executing `<Project Root>/shadowtraffic/run_pg.sh`.

- At this point you haev an incoming data stream into the PostgreSQL tables (`accountholders` and `transactions`).

- Next you want to create the various Flink catalogs and tables, after which you can start the embedding processing.
  
- Execute `devlab/pyflink/ah_embed.cmd` to start Embedding job on Flink Cluster of #1 data set / Account Holders.

- Execute `devlab/pyflink/txn_embed.cmd` to start Embedding job on Flink Cluster of #2 data set / Financial Transactions.

- Next is moving the data into one or other direction.
  - You can either move the data directly to a Iceberg or a Paimon based table.
  - You can push the data to Apache Fluss tables, which you can configure with tiering which will then move/tier the data onto:
    - Apache Iceberg or Apache Paimon.
  - Not shown is the option to add a Apache Kafka/Confluent based Kafka cluster, allowing the user to push the data onto Kafka Topics from where you can utlize the Kafka Connect Framework to sink the data into one of many many persistent store.s


## Stack

The following stack is deployed using one of the provided  `<Project Root>/devlab/docker-compose-*.yaml` files as per above.

- [Apache Flink 1.20.1](https://nightlies.apache.org/flink/flink-docs-release-1.20/)                   

- [Apache Flink CDC 3.5.0](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.5/)

- [Apache Paimon 1.3.1.](https://paimon.apache.org)

- [PostgreSQL 12](https://www.postgresql.org)

- [MinIO](https://www.min.io) - Project has gone into Maintenance mode... 

- [ShadowTraffic](https://shadowtraffic.io)


## Data Products 

Below is a overview of the data products we will create using Shadowtraffic, these will be inserted into our PostgreSQL CDC datastore.

From were they will be CDC source into our Flink environment into Flink tables that will be "consumed" using PyFlink jobs, first flattened and then as a 2nd job calculate vector embeddings (using different local [HuggingFace](https://huggingface.co) LLM models).


### 1. AccountHolders

```bash
_id
nationalid                                              => Random 16 Digits unique Number, excluded from embedding calc
    firstname
    lastname
    dob                                                 => YY/MM/DD   Min = current - 16yrs
    gender
    children
    address                                             => Can we drive addresses chosen based on country via .env value
    {
        # Number Street
        Suburb
        Town
        Provice/State
        Country
        Postal_code
    }
    accounts [
        # (1-5)                                         => .env driven
        Bank Account/s
            tenantId                                    => .env driven (PickList / or from PostgreSql Table of possible values)
                                                        => this becomes the tenantId, fromId, toId
            memberName                                  => .env driven (PickList / or from PostgreSql Table of possible values)
            bicFi                                       => .env driven (PickList / or from PostgreSql Table of possible values)
                                                        => this becomes the fromId or toId, possible same as tenantId
            brancId                                     => .env driven (PickList / or from PostgreSql Table of possible values)
                                                        => this become the fromBranchId or toFIBranchId
            accountId                                   => concat bicfi-<random-unique-16>
                                                        => this is either the accountId or counterPartyAccountId
            accountType                                 => .env driven (PickList / or from PostgreSql Table of possible values)
            accountOpenDate
        Credit Card/s
            issuingBank (tenantId)                      => .env driven (PickList / or from PostgreSql Table of possible values)
            bicFi                                       => .env driven (PickList / or from PostgreSql Table of possible values)
            cardNumber                                  => CC Structure, can we maybe pick first 4 chars from .env pick list (known as bin numbers) 
            cardType (VISA/MasterCard/Amex/DinerClub)   => .env driven (PickList / or from PostgreSql Table of possible values)
            expDate                                     => Current mm/year - <.env driven # months>
    ]
    emailaddress
    mobilephonenumber
    embedding_vector                                     => To be Calculated, Account Holder profile model 
    embedding_dimensions
    embedding_timestamp
    created_at

```

### 2. Financial Transactions

### Outbound Txn

```bash

    eventId                                             => UUIDv7   Unique, excluded from embedding calc
    transactionid                                       => UUIDv7   Shared with Inbound, excluded from embedding calc
        eventtime                                       => "2023-07-31T12:59:02"
        direction: outbound
        eventtype
        creationdate
        accountholdernationalid
        accountholderaccount                            => row
        counterpartynationalid
        counterpartyaccount                             => row
        tenantid
        fromid
        accountagentid
        fromfibranchid
        accountnumber
        toid
        accountidcode
        counterpartyagentid
        tofibranchid
        counterpartynumber
        counterpartyidcode
        verificationresult                              => .end driven (pick List)
        amount {                                        => row
            basecurrency                                => .env driven (pick List)
            basevalue
            roe                                         => .env driven 
            currency                                    => .env driven (pick List)
            value
        }
        msgType                                         => .end driven (pick List)
        settlementclearingsystemcode
        paymentclearingsystemreference
        requestexecutiondate
        settlementdate
        destinationcountry
        localinstrument
        msgstatus
        paymentmethod
        settlementmethod
        transactiontype
        verificationresult
        numberoftransactions
        schemaversion
        usercode
        embeddingVector                                 => To be Calculated, transaction profile model 
        embedding_dimensions
        embedding_timestamp
        created_at
    ```

### Inbound Txn 

(separate insert/record into Transaction table)

```bash

    eventId                                             => UUIDv7   Unique, excluded from embedding calc
    transactionid                                       => UUIDv7   Shared with Inbound, excluded from embedding calc
        eventtime                                       => "2023-07-31T12:59:02"
        direction: outbound
        eventtype
        creationdate
        accountholdernationalid
        accountholderaccount                            => row
        counterpartynationalid
        counterpartyaccount                             => row
        tenantid
        fromid
        accountagentid
        fromfibranchid
        accountnumber
        toid
        accountidcode
        counterpartyagentid
        tofibranchid
        counterpartynumber
        counterpartyidcode
        verificationresult                              => .end driven (pick List)
        amount {                                        => row
            basecurrency                                => .env driven (pick List)
            basevalue
            roe                                         => .env driven 
            currency                                    => .env driven (pick List)
            value
        }
        msgType                                         => .end driven (pick List)
        settlementclearingsystemcode
        paymentclearingsystemreference
        requestexecutiondate
        settlementdate
        destinationcountry
        localinstrument
        msgstatus
        paymentmethod
        settlementmethod
        transactiontype
        verificationresult
        numberoftransactions
        schemaversion
        usercode
        embeddingVector                                 => To be Calculated, transaction profile model 
        embedding_dimensions
        embedding_timestamp
        created_at
```


### By: George Leonard
- georgelza@gmail.com
- https://www.linkedin.com/in/george-leonard-945b502/
- https://medium.com/@georgelza



### More Reading



