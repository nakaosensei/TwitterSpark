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
from urllib3.exceptions import ProtocolError
producer = None

def preProcessData(data):
    data = data.replace('ç','c')
    data = data.replace('Ç', 'C')
    data = data.replace('á', 'a')
    data = data.replace('À', 'A')
    data = data.replace('à', 'a')
    data = data.replace('À', 'A')
    data = data.replace('ã', 'a')
    data = data.replace('Ã', 'A')
    data = data.replace('â', 'a')
    data = data.replace('Â', 'A')
    data = data.replace('è', 'e')
    data = data.replace('È', 'E')
    data = data.replace('é', 'e')
    data = data.replace('É', 'E')
    data = data.replace('ẽ', 'e')
    data = data.replace('Ẽ', 'E')
    data = data.replace('ê', 'e')
    data = data.replace('Ê', 'E')
    data = data.replace('ì', 'i')
    data = data.replace('Ì', 'I')
    data = data.replace('í', 'i')
    data = data.replace('Í', 'I')
    data = data.replace('ĩ', 'i')
    data = data.replace('Ĩ', 'I')
    data = data.replace('î', 'i')
    data = data.replace('Î', 'I')
    data = data.replace('ò', 'o')
    data = data.replace('Ò', 'O')
    data = data.replace('ó', 'o')
    data = data.replace('Ó', 'O')
    data = data.replace('õ', 'o')
    data = data.replace('Õ', 'O')
    data = data.replace('ô', 'o')
    data = data.replace('Ô', 'O')
    data = data.replace('ù', 'u')
    data = data.replace('Ù', 'U')
    data = data.replace('ú', 'u')
    data = data.replace('Ú', 'U')
    data = data.replace('ũ', 'u')
    data = data.replace('Ũ', 'U')
    data = data.replace('û', 'u')
    data = data.replace('Û', 'U')
    data = data.replace("...","")
    data = data.replace("…", "")
    data = data.replace(".", "")
    data = data.replace("?", "")
    data = data.replace("!", "")
    data = data.replace("R$", "reais")
    data = data.replace("{", "")
    data = data.replace("}", "")
    data = data.replace("%", "")
    data = data.replace("*", "")
    data = data.replace("(", "")
    data = data.replace(")", "")
    data = data.replace("+", "")
    data = data.replace("-", "")
    data = data.replace("=", "")
    data = data.replace("/", "")
    data = data.replace("#", "")
    data = data.replace("\n"," ")
    data = data.replace('"',"")
    out = ""

    messageSplit = data.split(' ')
    for m in messageSplit:
        if m!="RT" and 'http' not in m:
            out+=m+" "
    return out

class MyStreamListener(tweepy.StreamListener):

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_data disconnects the stream
            return False

    def on_status(self, status):
        try:
            print(preProcessData(status.text))
            f = open("logs.txt","a")
            f.write(preProcessData(status.text)+"\n")
            f.close()
            data = {'number': preProcessData(status.text)}
            producer.send('twitterEng', value=data)
            sleep(15)
        except:
            print("error")
            f = open("errors.txt", "a")
            f.write("ERROR\n")
            f.write(status.text)
            f.close()
            sleep(60)

if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x:dumps(x).encode('UTF-8'))
    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))

    myStreamListener = MyStreamListener()
    auth = tweepy.OAuthHandler('ZfyYT762f8iptmyOAQiPVpSSG', 'abdgza7zIl2lbRaYdY4L7C5zhGgxftLiACt5WJfrFtGnNIQqIF')
    auth.set_access_token('188507287-370vFKYm0sA6OumKatEWXRZuA4ippzCl7kteJCpi','MIaKNuYorBD5qqsxzzb58gOuvrBuGBToya5nzLEHsJ8kT')

    api = tweepy.API(auth)
    myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)
    while True:
        try:
            myStream.filter(track=['@jairbolsonaro', 'Jair Bolsonaro', 'jair bolsonaro', 'JAIR BOLSONARO'],languages=['en'],stall_warnings=True)
        except (ProtocolError, AttributeError):
            continue


