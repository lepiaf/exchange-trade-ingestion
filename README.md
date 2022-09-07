./bin/start-master.sh
./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 pyspark_job.py

https://stackoverflow.com/questions/61481628/spark-structured-streaming-with-kafka-sasl-plain-authentication
https://docs.microsoft.com/fr-fr/sql/big-data-cluster/spark-streaming-guide?view=sql-server-ver16
https://sparkbyexamples.com/pyspark/pyspark-parse-json-from-string-column-text-file/
https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks

```shell
EXCHANGE_SOURCE_URL=wss://ws-feed.exchange.coinbase.com \
EXCHANGE_LOGON_MSG='{"type": "subscribe","product_ids": ["BTC-EUR"],"channels": ["level2"]}' \
KAFKA_TOPIC=exchange_raw_trade \
EXCHANGE_SOURCE_NAME=coinbase \
python3 datasource.py
```

```shell
EXCHANGE_SOURCE_URL=wss://api.gemini.com/v2/marketdata \
EXCHANGE_LOGON_MSG='{"type": "subscribe","subscriptions":[{"name":"l2","symbols":["BTCEUR"]}]}' \
KAFKA_TOPIC=exchange_raw_trade \
EXCHANGE_SOURCE_NAME=gemini \
python3 datasource.py
```
