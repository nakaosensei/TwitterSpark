import os
import findspark
findspark.init('../TwitterSpark/lib/spark-2.4.3-bin-hadoop2.7')
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.3 pyspark-shell'

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext


if __name__ == "__main__":
    sc = SparkContext(appName="PythonStreamingReceiver")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 15)  # 2 second window

    kafkaStream = KafkaUtils.createDirectStream(ssc, ["newsPt"], {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'data-mining-group',
        'fetch.message.max.bytes': '15728640',
        'auto.offset.reset': 'largest'
    })

    lines = kafkaStream.map(lambda x: x[1])

    counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)
    #print(type(counts))
    counts.pprint()
    sqlContext = SQLContext(sc)
    readFile = sqlContext.read.json('news.json')
    readFile.show()


    readFile.printSchema()
    ssc.start()
    ssc.awaitTermination()
