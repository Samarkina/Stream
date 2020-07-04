# Bot detection with Spark Streaming

### Description of the project:

Ads! Ads are everywhere these days! Even though you might not like it or even use adblocker, the market is huge and so is desire to trick it. Fraud has multiple meanings, but the most common example of fraud is bot networks -- ads are being consumed (watched, clicked) by robots, not real users, but paid in full. What's worse, most bot networks are short lived, they work for several hours at most and then got blocked, but the damage is already done.

You're the new startup 'StopBot', that does blazingly fast analytics, unlike most other companies you're able to detect those bots in a matter of minutes, or even seconds. What's more you can do this accurately just by looking at [ip address, time, url of page with banner]. Companies like Woogle, Yeahoo and some others rely on you and happy to provide you with their data.

#### Bot detection
Bot detection algorithm: more than 20 requests per 10 seconds for single IP. IP should be whitelisted in 10 minutes if bot detection condition is not matched. It means IP should be deleted from Redis once host stops suspicious activity.  

System should handle up to 200 000 events per minute and collect user click rate and transitions for the last 10 minutes.

Please pay additional attention to the way how your window is implemented on DStream. For Structured Streaming you can use watermark over event_time, while for DStream it’s not available (window is built on ingestion time), so it should be implemented on top of DStream. This is the most tricky part of the project.

#### Data formats
 All data is supplied in form of (multiple) files that got dumped on filesystem, each event is JSON, each JSON on it's own line, with the above mentioned fields:
```
{
"type": "click",
"ip": "127.0.0.1",
"event_time": "1500028835",
"url": "https://blog.griddynamics.com/in-stream-processing-service-blueprint"
}
```

Fields to store in Cassandra:
```
{
"type": "click",
"ip": "127.0.0.1",
“is_bot”: true,
"event_time": "1500028835",
"url": "https://blog.griddynamics.com/in-stream-processing-service-blueprint"
}
```

#### Stack:
1. Kafka Connect as ingestion tool
2. Apache Kafka as a messaging backbone
3. Spark Streaming for processing
4. Cassandra as a storage for all incoming events, including bot events as well

#### Pipeline of the project:

[partners] => [file disk] => [kafka connect] => [kafka] => [spark] => [cassandra]



### How to run the project:

For running Zookeeper and Kafka
```
./runZookeeper.sh
./runKafka.sh
```

For migrating
```
./run.sh

```

Cassandra database
```
./runCassandra.sh
./runSelect.sh
```

For removing and creating new data
```
./kafka-connect/data.sh
```

```
kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic kafkaConnectStandalone 
```
