### ./bin/start-micro-quickstart 




### ./bin/zookeeper-server-start.sh config/zookeeper.properties (note that if you run druid, druid startup launches zookeeper anyways, so you have to start duid first if you want to run both)
### ./bin/kafka-server-start.sh config/server.properties

### ./bin/kafka-topics.sh --create --topic coinmetrics-markets --partitions 10 --replication-factor 1 --bootstrap-server localhost:9092
### ./bin/kafka-topics.sh --delete --topic coinmetrics-markets --bootstrap-server localhost:9092

### ./bin/kafka-console-consumer.sh --topic coinmetrics-markets --from-beginning --bootstrap-server localhost:9092





### sudo apt-get install pkg-config libssl-dev
### RUST_LOG=DEBUG cargo --bin build WHATEVER
### db.crypto.createIndex({'trade_date': 1})
### db.crypto.createIndex({'trade_date': 1, 'tx_type': 1})
### db.crypto.createIndex({'trade_date': 1, 'tx_type': 1, 'market': 1})
### db.crypto.createIndex({'trade_date': 1, 'tx_type': 1, 'trade_llama_instrument_type': 1})
