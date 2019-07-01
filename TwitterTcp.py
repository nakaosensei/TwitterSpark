import tweepy
import socket

auth = tweepy.OAuthHandler('ZfyYT762f8iptmyOAQiPVpSSG', 'abdgza7zIl2lbRaYdY4L7C5zhGgxftLiACt5WJfrFtGnNIQqIF')
auth.set_access_token('188507287-370vFKYm0sA6OumKatEWXRZuA4ippzCl7kteJCpi', 'MIaKNuYorBD5qqsxzzb58gOuvrBuGBToya5nzLEHsJ8kT')

api = tweepy.API(auth)
public_tweets = api.home_timeline()

#Ultimos 20 tweets de jairbolsonaro
bolsonaro = api.get_user("@jairbolsonaro")
print(bolsonaro.id)
print(bolsonaro.screen_name)
timeline = api.user_timeline(bolsonaro.id)
#for element in timeline:
#    print(element.text)
results = api.search(q="jairbolsonaro",language="en-us")

TCP_IP = "localhost"
TCP_PORT = 9009
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting tweets.")

for element in results:
    print(element.text)
    conn.send((element.text+' \n').encode())







