# kafka-init.sh
#!/bin/bash
kafka-topics --bootstrap-server kafka:9092 --alter --topic data_topic --partitions 8
kafka-topics --bootstrap-server kafka:9092 --create --topic data_consumer-last_message-changelog --if-not-exists --partitions 8 --replication-factor 1
