
## Random Notes


- [ShadowTraffic](https://docs.shadowtraffic.io/overview/)
  

### Run

Execute `run_pg.sh`, this will run the shadotraffic container connected to the host network, allowing localhost as host for the PostgreSQL datastore.

- The simulation configuration is as per the `--config /home/config.json` configuration.

    Take note of the `"throttleMs": 1000` as part of each generator blocks `localconfigs` section, just to slow things down a bit during development

- License key in `conf/license.env`
