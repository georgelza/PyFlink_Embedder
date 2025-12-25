## Vector Embedding of Account Holders & Financial Transactions in the Realtime Transaction world, at volume


1. Generate all data Using Shadowtraffic => into PostgreSQL

2. Consume from PostgreSQL => Apacke Flink tables using CDC

3. We have two PyFlink based vector embedding User Defined Functions (UDF's) to Calculate vector embedding values.

4. These are used as part of a Insert into `c_paimon.finflow.<target table>` select (fields..., generate_ah_embedding(fields)) from `c_cdcsource.demog.<source table>`;


BLOG: [Using Pyflink UDF to calculate embedding vectors on inbound tables via Flink CDC]()

GIT REPO: [PyFlink_Embedder](https://github.com/georgelza/PyFlink_Embedder.git)


## Deployment

- `<Project Root>/devlab/docker-compose.yml` which can be brought online by executing below, (this will use `.env`).

- Execute `make run` as defined in `devlab/Makefile` to run environment.
  
- Deploy Catalog `make run`       -> Run MinIO/S3 based version.

- Deploy Catalog `make run-fs`    -> Run Filesystem based version.

- Deploy Stack `make deploy`      -> Run MinIO/S3 based version.

- Deploy Stack `make deploy-fs`   -> Run Filesystem based version.

- Deploys accountHolder embedding flow

  -  `make ahs`
  
- Deploys transaction embedding flow

  -  `make txns`

- Execute `Shadowtraffic` to create Workload for our #1 AccountHolders, #2 Financial Transactions tables.

  - => Output to 2 PostgreSQL Tables located in postgrescdc Postgres based database/service.

  - This is done by executing `<Project Root>/shadowtraffic/run_pg1.sh`.

  - If you want to increase the data generate rate execute `<Project Root>/shadowtraffic/run_pg2.sh`.

## Summary

At this point you have an incoming data stream into the PostgreSQL tables (`accountholders` and `transactions`).

From here it is consumed, embedding vectors calculated and pushed out to our Apache Paimon based Lakehouse.

### S3/ MinIO

<img src="blog-doc/diagrams/SuperLab-minio-v1.2.png" alt="Our Build" width="600">

### Filesystem
  
<img src="blog-doc/diagrams/SuperLab-fs-v1.2.png" alt="Our Build" width="600">

## Regarding our Stack

The following stack is deployed using one of the provided  `<Project Root>/devlab/docker-compose-*.yaml` files as per above.

- [Apache Flink 1.20.2](https://nightlies.apache.org/flink/flink-docs-release-1.20/)                   

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
_id                                                     => Sequentially incrementing value
nationalid                                              => Random 16 Digits unique Number, excluded from embedding calc
    firstname
    lastname
    dob                                                 => YY/MM/DD   Min = current - 16yrs
    gender
    children
    address                                             => Can we drive addresses chosen based on country via .env value 
    {
        Street Address
        Suburb
        Town
        Province
        Country
        Postal Code
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

### Outbound Txn: From Payer to Payee

```bash
    _id                                                 => Sequentially incrementing value
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

### Inbound Txn: To Payee from Payer

(separate insert/record into Transaction table)

```bash
    _id                                                 => Sequentially incrementing value
    eventId                                             => UUIDv7   Unique, excluded from embedding calc
    transactionid                                       => UUIDv7   Shared with Inbound, excluded from embedding calc
        eventtime                                       => "2023-07-31T12:59:02"
        direction: inbound
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


<img src="blog-doc/diagrams/rabbithole.jpg" alt="Our Build" width="600">


### By: George Leonard


- georgelza@gmail.com
- https://www.linkedin.com/in/george-leonard-945b502/
- https://medium.com/@georgelza



<img src="blog-doc/diagrams/TechCentralFeb2020-george-leonard.jpg" alt="Our Build" width="600">




