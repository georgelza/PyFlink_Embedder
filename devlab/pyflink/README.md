## Deploying our Pyflink UDF's(User Defined Functions.)

### Deployment

I have designed the embedding "example" using two different patterns.

- The first is an `Insert into <target table> (<columns>) as select (<columns>) from source table;`
  
  For this option we include a call to the generate_ah_embedding() UDF as part of the select block, providing the function with the required field values.

  - This is accomplished by deploying the script as per `<Project root>/devlab/creFlinkFlows/scripts/4.1.creInsertsAh.sql`, You can either copy/past the commands contained in the sql script, or

  - xxx  or 
  
  - by executing `make ins_ah` from withing the `<Project root>/devlab/` directory.


- The second is a standalone Pyflink job executed at the `Jobmanager` CLI from within the `/pyflink/` directory.
  
  - `make jm`

  - `cd /pyflink`
  
  - copy command from `txn_embed.cmd` into jobmanager terminal and hit ENTER.
  
  - or by executing `make ins_txn` from withing the `<Project root>/devlab/` directory.




### Flattening our json structured "string" column 

This is accomplished using an awesome package written and made available by [Hans-Peter Grahsl](https://www.linkedin.com/in/hpgrahsl/) ...

We will consume our inboundPostgreSQL source tables, take the the original jsonb columns, that are now represented as `string` columns in our Flink CDC table and convert this back into a complex structured row or array of rows or unwind'd into a new table.

Git Repo: [Flink-json-udf](https://github.com/hpgrahsl/flink-json-udfs)

