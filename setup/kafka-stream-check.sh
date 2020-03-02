kafka-console-consumer.sh --bootstrap-server kafka-m:9092 --topic readings_raw
kafka-console-consumer.sh --bootstrap-server kafka-m:9092 --topic readings_prep
kafka-console-consumer.sh --bootstrap-server kafka-m:9092 --topic alert_1_stats
kafka-console-consumer.sh --bootstrap-server kafka-m:9092 --topic alert_2_stats