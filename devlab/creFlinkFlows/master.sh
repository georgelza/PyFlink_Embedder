
# You can enter jomanager by using `make jm` then execute master.sh which in turn will execute master.sql


# Remove environment parameters from the master.sql and place in a common configuration file
#
./bin/sql-client.sh -f /creFlinkFlows/master.sql -s /creFlinkFlows/config/sql-client-conf.yaml