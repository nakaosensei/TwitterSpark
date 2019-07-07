import tweepy
import socket
#override tweepy.StreamListener to add logic to on_status

TCP_IP = "localhost"
TCP_PORT = 9009
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()


class MyStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        print(status.text)
        conn.send((status.text + ' \n').encode("UTF-8"))


myStreamListener = MyStreamListener()
auth = tweepy.OAuthHandler('ZfyYT762f8iptmyOAQiPVpSSG', 'abdgza7zIl2lbRaYdY4L7C5zhGgxftLiACt5WJfrFtGnNIQqIF')
auth.set_access_token('188507287-370vFKYm0sA6OumKatEWXRZuA4ippzCl7kteJCpi', 'MIaKNuYorBD5qqsxzzb58gOuvrBuGBToya5nzLEHsJ8kT')

api = tweepy.API(auth)
myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)
myStream.filter(track=['@jairbolsonaro'])