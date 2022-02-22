## Requirements local

### .env file in src

LOCAL_MONGO=mongodb://localhost:27017
TLDB=tradellama

### Rust

https://doc.rust-lang.org/cargo/getting-started/installation.html

curl https://sh.rustup.rs -sSf | sh

### Mongo

Used to track runs against coinmetrics, but will probably use for other things as well...NOT NEEDED FOR DYDX

https://docs.mongodb.com/manual/tutorial/install-mongodb-on-ubuntu/

### Kafka

sudo apt install default-jdk

https://kafka.apache.org/quickstart

### start kafka

./bin/zookeeper-server-start.sh config/zookeeper.properties

in background

nohup ./bin/zookeeper-server-start.sh config/zookeeper.properties &

./bin/kafka-server-start.sh config/server.properties

in background

nohup ./bin/kafka-server-start.sh config/server.properties &


### Stuff to do with Kafka

./bin/kafka-topics.sh --create --topic coinmetrics-markets --partitions 10 --replication-factor 1 --bootstrap-server localhost:9092

./bin/kafka-topics.sh --create --topic kraken-markets --partitions 10 --replication-factor 1 --bootstrap-server localhost:9092

./bin/kafka-topics.sh --create --topic dydx-markets --partitions 10 --replication-factor 1 --bootstrap-server localhost:9092 --config retention.ms=86400000

note the above has retention of 24 hours, so need to run consumer in that window - at least once daily


./bin/kafka-topics.sh --create --topic phemex-perpetuals-open-interest --partitions 10 --replication-factor 1 --bootstrap-server localhost:9092

./bin/kafka-topics.sh --delete --topic coinmetrics-markets --bootstrap-server localhost:9092

./bin/kafka-topics.sh --delete --topic kraken-markets --bootstrap-server localhost:9092

./bin/kafka-topics.sh --delete --topic dydx-markets --bootstrap-server localhost:9092

./bin/kafka-topics.sh --delete --topic phemex-perpetuals-open-interest --bootstrap-server localhost:9092

./bin/kafka-console-consumer.sh --topic coinmetrics-markets --from-beginning --bootstrap-server localhost:9092

./bin/kafka-console-consumer.sh --topic kraken-markets --from-beginning --bootstrap-server localhost:9092

./bin/kafka-console-consumer.sh --topic dydx-markets --from-beginning --bootstrap-server localhost:9092

./bin/kafka-console-consumer.sh --topic dydx-markets --from-beginning --property print.key=true --bootstrap-server localhost:9092

./bin/kafka-console-consumer.sh --topic phemex-perpetuals-open-interest --from-beginning --bootstrap-server localhost:9092


### Druid (not needed to run, just for consumption options)

Druid - not that when you run druid in console, it launches zookeeper (if you ran Kafka by itself, Kafka also has zookeeper, and directs you to launch it)
https://druid.apache.org/docs/latest/tutorials/index.html


### Superset  (not needed to run, just for consumption options)

note that I cloned from git, did the install, and now, boot reboot, it always seems to be running
https://superset.apache.org/docs/installation/installing-superset-using-docker-compose


### start mongo
sudo systemctl start mongod

### start druid - best on another machine
./bin/start-micro-quickstart 


### Running DYDX - loop for quotes
RUST_LOG=DEBUG cargo run --bin entry all-markets-dydx

RUST_LOG=DEBUG cargo run --bin entry orderbooks-dydx



### Misc

sudo apt-get install pkg-config libssl-dev  

RUST_LOG=DEBUG cargo --bin build WHATEVER

db.tldydxsnap.createIndex({'mongo_snapshot_date': 1})

db.tldydxsnap.createIndex({'market': 1})

db.tldydxsnap.createIndex({'mongo_snapshot_date':1, 'market': 1})