1. Run each producer script separately to start streaming data into Kafka topics.
    python kafka_producers/json_producer.py
    python kafka_producers/csv_producer.py
    python kafka_producers/avro_producer.py

2. Run the Spark Streaming job to process data from Kafka and write it to S3.
    spark-submit spark_processing/process_streaming.py

3. Start Prometheus with the configuration file.
    prometheus --config.file=monitoring/prometheus.yml

