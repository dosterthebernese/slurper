### What This Is

The reason for the repo is to create various endpoint consumers of exchanges (decentralized and centralized), as well as data providers, and write to local Kafka producers.  The Kafka data can be consumed by Druid, for visualization in Superset.

Additionally, their are calls to consume Kafka locally into Mongo, for aggregations and clustering, that then are also written to Kafka, for the above mentioned Druid / Superset usage.

Kafka gives the live stream to druid, Mongo allows us to be creative with the Kafka data, and persist longer than we might want to with Kafka.

The mongo writes should be batched, but for now, it's just a beta.  

### Requirements local

mongo (default install) - used to track runs against coinmetrics, but will probably use for other things as well

https://docs.mongodb.com/manual/tutorial/install-mongodb-on-ubuntu/


Kafka - see above, don't run the zookeeper launch
https://kafka.apache.org/quickstart


### Druid

Don't run on the above machine, you're asking for trouble.

Druid - not that when you run druid in console, it launches zookeeper (if you ran Kafka by itself, Kafka also has zookeeper, and directs you to launch it)
https://druid.apache.org/docs/latest/tutorials/index.html


### Superset 
note that I cloned from git, did the install, and now, boot reboot, it always seems to be running
https://superset.apache.org/docs/installation/installing-superset-using-docker-compose


### start druid - best on another machine
./bin/start-micro-quickstart 

./bin/zookeeper-server-start.sh config/zookeeper.properties

./bin/kafka-server-start.sh config/server.properties


./bin/kafka-topics.sh --create --topic coinmetrics-markets --partitions 10 --replication-factor 1 --bootstrap-server localhost:9092

./bin/kafka-topics.sh --create --topic kraken-markets --partitions 10 --replication-factor 1 --bootstrap-server localhost:9092

./bin/kafka-topics.sh --create --topic dydx-markets --partitions 10 --replication-factor 1 --bootstrap-server localhost:9092

./bin/kafka-topics.sh --create --topic phemex-perpetuals-open-interest --partitions 10 --replication-factor 1 --bootstrap-server localhost:9092

./bin/kafka-topics.sh --delete --topic coinmetrics-markets --bootstrap-server localhost:9092

./bin/kafka-topics.sh --delete --topic kraken-markets --bootstrap-server localhost:9092

./bin/kafka-topics.sh --delete --topic dydx-markets --bootstrap-server localhost:9092

./bin/kafka-topics.sh --delete --topic phemex-perpetuals-open-interest --bootstrap-server localhost:9092

./bin/kafka-console-consumer.sh --topic coinmetrics-markets --from-beginning --bootstrap-server localhost:9092

./bin/kafka-console-consumer.sh --topic kraken-markets --from-beginning --bootstrap-server localhost:9092

./bin/kafka-console-consumer.sh --topic dydx-markets --from-beginning --bootstrap-server localhost:9092

./bin/kafka-console-consumer.sh --topic phemex-perpetuals-open-interest --from-beginning --bootstrap-server localhost:9092





sudo apt-get install pkg-config libssl-dev
RUST_LOG=DEBUG cargo --bin build WHATEVER


db.crypto_trade.createIndex({'trade_date': 1})

db.crypto_trade.createIndex({'trade_date': 1, 'tx_type': 1})

db.crypto_trade.createIndex({'trade_date': 1, 'tx_type': 1, 'market': 1})

db.crypto_trade.createIndex({'trade_date': 1, 'tx_type': 1, 'trade_llama_instrument_type': 1})
