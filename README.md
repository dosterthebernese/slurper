### Requirements local
## mongo (default install) - used to track runs against coinmetrics, but will probably use for other things as well
## https://docs.mongodb.com/manual/tutorial/install-mongodb-on-ubuntu/
## Druid - not that when you run druid in console, it launches zookeeper (if you ran Kafka by itself, Kafka also has zookeeper, and directs you to launch it)
## https://druid.apache.org/docs/latest/tutorials/index.html
## Kafka - see above, don't run the zookeeper launch
## https://kafka.apache.org/quickstart
## Superset 
## note that I cloned from git, did the install, and now, boot reboot, it always seems to be running
## https://superset.apache.org/docs/installation/installing-superset-using-docker-compose

### components
## start with Phemex - neil's account secret is hardcoded, so, any other users, we need to change that, and I need your IP (external to the world) so I can login and create an additional secret
## starting with Phemex is easiest, and the default "loop" just grabs open interest of various perps - so we could easily in superset show those trends, agains spot prices, for very easy to understand demo of how we grab data and enable signals across various DeFi players (not that Phemex is DeFi)


## ./bin/start-micro-quickstart 

## ./bin/zookeeper-server-start.sh config/zookeeper.properties (note that if you run druid, druid startup launches zookeeper anyways, so you have to start duid first if you want to run both)
## ./bin/kafka-server-start.sh config/server.properties

## ./bin/kafka-topics.sh --create --topic coinmetrics-markets --partitions 10 --replication-factor 1 --bootstrap-server localhost:9092
## ./bin/kafka-topics.sh --create --topic dydx-markets --partitions 10 --replication-factor 1 --bootstrap-server localhost:9092
## ./bin/kafka-topics.sh --delete --topic coinmetrics-markets --bootstrap-server localhost:9092

## ./bin/kafka-console-consumer.sh --topic coinmetrics-markets --from-beginning --bootstrap-server localhost:9092
## ./bin/kafka-console-consumer.sh --topic dydx-markets --from-beginning --bootstrap-server localhost:9092





### sudo apt-get install pkg-config libssl-dev
### RUST_LOG=DEBUG cargo --bin build WHATEVER

### Ignore this shit for now
## db.crypto.createIndex({'trade_date': 1})
## db.crypto.createIndex({'trade_date': 1, 'tx_type': 1})
## db.crypto.createIndex({'trade_date': 1, 'tx_type': 1, 'market': 1})
## db.crypto.createIndex({'trade_date': 1, 'tx_type': 1, 'trade_llama_instrument_type': 1})
