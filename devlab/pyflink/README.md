## Deploying our Pyflink User Defined Functions.


### Execute our 2 Pyflink functions producing embedding values

We will consume our inbound PostgreSQL source tables, calculate embedidng values and output into 2 Paimon based tables.

- ah_embed_cmd
- txn_embed.cmd

The Apache Flink hosted Pyflink jobs are executed using:

(from within <project root>/devlab directory):

- `make jm`
- `cd /pyflink`
- copy command from `*_embed.cmd` into jobmanager terminal and hit ENTER


### Flattening our json structured "string" column 


This is accomplished using an awesome package written and made available by [Hans-Peter Grahsl](https://www.linkedin.com/in/hpgrahsl/) ...

We will consume our inboundPostgreSQL source tables, take the the original jsonb columns, that are now represented as `string` columns in our Flink CDC table and convert this back into a complex structured row or array of rows or unwind'd into a new table.

Git Repo: [Flink-json-udf](https://github.com/hpgrahsl/flink-json-udfs)

