import os
import findspark
findspark.init('../TwitterSpark/lib/spark-2.4.3-bin-hadoop2.7')
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.3 pyspark-shell'
# initiate Apache Kafka
# download link: https://www.apache.org/dyn/closer.lua/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.7.tgz
# extract downloaded file and put path to it in the following line

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from uuid import uuid1


if __name__ == "__main__":
    sc = SparkContext(appName="PythonStreamingReceiver")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 15)  # 2 second window

    # twitter_eng_stream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'twitterEng': 1})
    # twitter_pt_stream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'twitterPt': 1})

    kafkaStream = KafkaUtils.createDirectStream(ssc, ["twitterEng"], {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'data-mining-group',
        'fetch.message.max.bytes': '15728640',
        'auto.offset.reset': 'largest'
    })

    lines = kafkaStream.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line.split(" ")) \
                  .map(lambda word: (word, 1)) \
                  .reduceByKey(lambda a, b: a+b)

    counts.pprint()
    ssc.start()
    ssc.awaitTermination()
