#!/bin/bash
# run_with_pyflink.sh

export PYFLINK_CLIENT_EXECUTABLE=/usr/bin/python3

SQL_CLIENT="/opt/flink/bin/sql-client.sh"

# Set Python path for UDFs
export PYTHONPATH="/pyflink/udfs:$PYTHONPATH"

# Execute with Python configuration
$SQL_CLIENT \
  -pyexec /usr/bin/python3 \
  -pyfs file:///pyflink/udfs \
  -f master.sql
```

## **Complete Example Structure**
```
project/
│
├── scripts/
│   ├── 02_register_ah_embed_udfs.sql
│   └── 03_register_txn_embed_udfs.sql
│
├── udfs/
│   ├── 02_ah_embed_udf.py
│   └── 03_txn_embed_udf.py
│
├── ah_embed.cmd
├── txn_embed.cmd
├── master.sh
├── master.sql
└── README.md
```

## NOTE: Tables are created via devlab/creFlinkFlows/master.sql also mounted onto Jobmanager into /creFlinkFlows/master.sql