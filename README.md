## Vector Embedding of Account Holders & Financial Transactions in the Realtime Transaction world, at volume


1. Generate all data Using Shadowtraffic => into PostgreSQL

2. Consume from PostgreSQL => Apacke Flink tables using CDC

3. Flatten using PyFlink Function 1 -> New Tables

4. Calculate vector embedding values using PyFlink, comsuming New tables from #3. -> Function 2 (AccountHolders) and Function 3 (Transactions) => Output to new Flink Table

5. -> Push to Iceberg Tables

6. -> Push to Apache Paimon Tabes


BLOG: []()

GIT REPO: []()


## Deployment

- `devlab/docker-compose.yml` which can be brought online by executing below, (this will use `.env`).

- Execute `make run` as defined in `devlab/Makefile` to run envirnment
  
- Execute Shadowtraffic to create Workload (#1 AccountHolders, #2 Financial Transactions) => Output to 2 PostgreSQL Tables.

- Execute <xxx> to start Embedding job on Flink Cluster of #1 data set / Account Holders.

- Execute <yyy> to start Embedding job on Flink Cluster of #2 data set / Financial Transactions.


## Stack

The following stack is deployed using the `docker-compose.yaml` file as per above.

- [ShadowTraffic](https://shadowtraffic.io)
- [Apache Flink 1.20.2](https://nightlies.apache.org/flink/flink-docs-release-1.20/)                   
- [Apache Flink CDC 3.5](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.5/)
- [Python 3.13](https://www.python.org)
- [Apache Fluss 0.8](https://fluss.apache.org)
- [Apache Iceberg 1.9.1](https://iceberg.apache.org)
- MinIO 
- [PostgreSQL 12](https://www.postgresql.org)
- [Apache Polaris 1.2.0]()


## Data Products 

Below is a overview of the data products we will create using Shadowtraffic, these will be inserted into our PostgreSQL CDC datastore.

From were they will be CDC source into our Flink environment into Flink tables that will be "consumed" using PyFlink jobs, first flattened and then as a 2nd job calculate vector embeddings (using different local [HuggingFace](https://huggingface.co) LLM models).


### 1. AccountHolders

```bash

idNumber/PPS/SSN                                        => Random 16 Digits unique Number, excluded from embedding calc
    firstname
    lastname
    dob                                                 => YY/MM/DD   Min = current - 16yrs
    gender
    address                                             => Can we drive addresses chosen based on country via .env value
    {
        # Number Street
        Suburb
        Town
        Provice/State
        Country
        Postal_code
    }
    eMailAddress
    MobilePhoneNumber
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
    embeddingVector                                     => To be Calculated, Account Holder profile model 

```

### 2. Financial Transactions

### Outbound Txn

```bash

    eventId                                             => UUIDv7   Unique, excluded from embedding calc
    transactionid                                       => UUIDv7   Shared with Inbound, excluded from embedding calc
        msgType                                         => .end driven (pick List)
        verificationresult                              => .end driven (pick List)
        amount {
            basecurrency                                => .env driven (pick List)
            basevalue
            roe                                         => .env driven 
            currency                                    => .env driven (pick List)
            value
        }
        eventtime                                       => "2023-07-31T12:59:02"
        direction: outbound
        accountholder (A)                               => Payer
            tenantid                                    => bicFi
            fromid                                      => bicFI
            Bank Account or Credit Card                 => random from account types
            accountholder                               => Name Surname
            accountid                                   => accountId
        counterpartyaccountholder (B)                   => Payee
            counterpartyagentid                         => bicFi
            counterpartybranchid                        => branchId 
            toid                                        => bicFi
            Bank Account or Credit Card                 => random from card types
            counterpartyaccountholder                   => Name Surname
            counterpartyaccountid                       => accountId
        embeddingvector                                 => To be Calculated
```

### Inbound Txn 

(separate insert/record into Transaction table)

```bash

    eventId                                             => UUIDv7   Unique, excluded from embedding calc
    transactionId                                       => UUIDv7   Shared with Outbound, excluded from embedding calc
        msgType                                         => from Outbound
        verificationResult                              => from Outbound
        amount {                                        => from Outbound
            baseCurrency
            baseValue
            RoE
            currency
            value
        }
        eventTime                                       => "2023-07-31T12:59:02"
        direction: inbound
        accountHolder (B)                               => Payee
            tenantId
            toId
            Bank Account or Credit Card
            accountHolder
            accountId
        counterpartyAccountHolder (A)                   => Payer
            counterpartyAgentId                         => bicFi
            counterPartyBranchId                        => branchId 
            fromId                                      => bicFi
            Bank Account or Credit Card                 => 
            counterPartyAccountHolder                   => Name Surname
            counterPartyAccountId                       => accountId
        embeddingVector                                 => To be Calculated / fin model
```


### By: George Leonard
- georgelza@gmail.com
- https://www.linkedin.com/in/george-leonard-945b502/
- https://medium.com/@georgelza



### More Reading



