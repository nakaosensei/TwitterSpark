from datetime import datetime, timedelta
from newsapi import NewsApiClient
import utils
import keys
import json
import time

from datetime import date

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




def stream():
    while True:
        time_now = utils.date_to_string(datetime.now())
        time_last_15min = utils.date_to_string(datetime.now() - timedelta(minutes=15))
        articles = request_news(from_date = time_last_15min,to_date = time_now, language = 'pt')
        extend_news(articles)
        print(f"{articles['totalResults']} acquired")
        with open(file, "w") as write_file:
            json.dump(articles, write_file)
        time.sleep(utils.minutes_15)

if __name__ == "__main__":
    stream()


