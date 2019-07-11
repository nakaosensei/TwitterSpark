from confluent_kafka import Producer
import sys
import socket
from time import sleep
from json import dumps
from kafka import KafkaProducer
import tweepy
from keys import tweepyOauth
from keys import tweepyToken
from urllib3.exceptions import ProtocolError

from datetime import datetime, timedelta
from newsapi import NewsApiClient
import utils
import keys
import json
import time

producer = None

file = "files/temp.json"

def request_news(from_date,to_date,language,sort_by = 'popularity',page_size=100,pos_page=1):
    newsapi = NewsApiClient(api_key = keys.newsapi_key)
    return newsapi.get_everything(q="\"Jair Bolsonaro\"",
                                      from_param=from_date,
                                      to = to_date,
                                      language = language,
                                      sort_by = sort_by,
                                      page_size = page_size,
                                      page = pos_page)

def extend_news(new_articles):
    try:
        with open(file, "r") as read_file:
            past_articles = json.load(read_file, encoding = "utf-8")
        new_articles["articles"].extend(past_articles["articles"])
    except:
        return new_articles


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
    data = data.replace("…","")
    data = data.replace('ù', 'u')
    data = data.replace('Ù', 'U')
    data = data.replace('ú', 'u')
    data = data.replace('Ú', 'U')
    data = data.replace('ũ', 'u')
    data = data.replace('Ũ', 'U')
    data = data.replace('û', 'u')
    data = data.replace('Û', 'U')
    data = data.replace("...","")
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


if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x:dumps(x).encode('UTF-8'))
    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))
    while True:
        time_now = utils.date_to_string(datetime.now())
        time_last_15min = utils.date_to_string(datetime.now() - timedelta(minutes=15))
        articles = request_news(from_date=time_last_15min, to_date=time_now, language='pt')
        data = articles['articles']
        print("Artigos")
        for artigo in data:
            print(artigo)
            producer.send('newsPt', value=preProcessData(artigo.description)+"\n")
        extend_news(articles)
        print(f"{articles['totalResults']} acquired")
        with open("news2.json", "w") as write_file:
            json.dump(articles, write_file)
        time.sleep(utils.minutes_15)



    '''
    myStreamListener = MyStreamListener()
    auth = tweepy.OAuthHandler(tweepyOauth[0],tweepyOauth[1])
    auth.set_access_token(tweepyToken[0],tweepyToken[1])
    api = tweepy.API(auth)
    myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)
    while True:
        try:
            myStream.filter(track=['@jairbolsonaro', 'Jair Bolsonaro', 'jair bolsonaro', 'JAIR BOLSONARO'],languages=['pt'],stall_warnings=True)
        except (ProtocolError, AttributeError):
            continue
    '''

