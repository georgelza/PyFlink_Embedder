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
│   ├── README.md
│   ├── register_ah_embed_udfs.sql
│   ├── register_txn_embed_udfs.sql
│   └── test_ah_udf.sql
│
├── udfs/
│   ├── ah_embed_udf.py
│   ├── README.md
│   └── txn_embed_udf.py
│
├── master.sh
├── master.sql
├── README.md
├── README1.md
└── txn_embed.cmd
```

## NOTE: Tables are created via devlab/creFlinkFlows/master.sql also mounted onto Jobmanager into /creFlinkFlows/master.sql