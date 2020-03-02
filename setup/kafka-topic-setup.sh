# 20 minutes retention because I don't need longer anyway
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --config retention.ms=1200000 --topic readings_raw
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --config retention.ms=1200000 --topic readings_prepared
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --config retention.ms=1200000 --topic alert_1_stats
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --config retention.ms=1200000 --topic alert_2_stats