#!/usr/bin/env python
#
# Copyright 2016 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Example Kafka Producer.
# Reads lines from stdin and sends to Kafka.
#

from confluent_kafka import Producer
import sys
import socket
from time import sleep
from json import dumps
from kafka import KafkaProducer
import tweepy

producer = None

class MyStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        print(status.text)
        data = {'number': status.text}
        producer.send('twitterEng', value=data)
        sleep(5)


if __name__ == '__main__':
    # if len(sys.argv) != 3:
    #     sys.stderr.write('Usage: %s <bootstrap-brokers> <topic>\n' % sys.argv[0])
    #     sys.exit(1)

    # Producer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

    # Create Producer instance
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x:dumps(x).encode('utf-8'))
    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).

    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))

    # Read lines from stdin, produce each line to Kafka

    myStreamListener = MyStreamListener()
    auth = tweepy.OAuthHandler('ZfyYT762f8iptmyOAQiPVpSSG', 'abdgza7zIl2lbRaYdY4L7C5zhGgxftLiACt5WJfrFtGnNIQqIF')
    auth.set_access_token('188507287-370vFKYm0sA6OumKatEWXRZuA4ippzCl7kteJCpi',
                          'MIaKNuYorBD5qqsxzzb58gOuvrBuGBToya5nzLEHsJ8kT')

    api = tweepy.API(auth)
    myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)

    myStream.filter(track=['@jairbolsonaro'])

    '''TCP_IP = "localhost"
    TCP_PORT = 9009
    conn = None
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((TCP_IP,TCP_PORT))
    s.send(b"Logging in")

    while True:
        received = s.recv(1024).decode("UTF-8")
        if received is not None:
            data = {'number':received}
            producer.send('twitterEng', value=data)
            sleep(1)
    '''
    '''for e in range(1000):
        try:
            # Produce line (without newline)


            data = {'number': 'bolsonaro oioi'}
            print("sendinggg")
            producer.send('twitterEng', value=data)
            sleep(2)
        except BufferError:
            sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n')
    '''
        # Serve delivery callback queue.
        # NOTE: Since produce() is an asynchronous API this poll() call
        #       will most likely not serve the delivery callback for the
        #       last produce()d message.

    p.flush()
