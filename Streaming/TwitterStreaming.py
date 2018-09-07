
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import sys
import Queue
import time
import socket
import httplib
import threading
import random, json



tcpip_delay = 0.25
MAX_TCPIP_TIMEOUT = 16
http_delay = 5
MAX_HTTP_TIMEOUT = 320


consumer_key=sys.argv[1]
consumer_secret=sys.argv[2]


access_token=sys.argv[3]
access_token_secret=sys.argv[4]
tmp_list = []
hash_tag=[]
class SListener(StreamListener):


    def __init__(self):

        super(SListener, self).__init__()
        self.num_handled = 0
        self.queue_txt = Queue.Queue()
        self.queue_id= Queue.Queue()
        self.queue_tags = Queue.Queue()

    def on_status(self, status):
        #print(status.extended_tweet['full_text'])
        # ( status.entities.get('hashtags'))
        if not 'RT @' in status.text:
            self.queue_txt.put(status.text)
            self.queue_id.put(status.id_str)
            self.queue_tags.put(status.entities)

            self.num_handled += 1

        return True

    def on_error(self, status):
        print(status)
        sys.exit(0)

    def on_delete(self, status_id, user_id):


        return



def avg_len(a):
    l=0
    for i in a :
        unicode_string= i.encode("utf-8")
        l+=len(unicode_string)

    return float(l)/float(len(a))

def top_five(list1):
    hashtags_dict=dict()
    result=[]
    for hashtag in list1:  # type: object
        for h in hashtag:
            if h['text'] in hashtags_dict.keys():
                 hashtags_dict[h['text']] += 1

            else:
                 hashtags_dict[h['text']] = 1

    keys=sorted(hashtags_dict, key=hashtags_dict.get, reverse=True)[:5]
    for k in keys:
         result.append((k,hashtags_dict[k]))

    return result



def worker(listener, flush_every=500):

    count = 0
    s=0

    hashtag=None

    while True:

        data = listener.queue_txt.get()


        entities= listener.queue_tags.get()

        if "hashtags" in entities and entities["hashtags"] is not None :

                if entities["hashtags"]:


                            if len(tmp_list)<100:
                                tmp_list.append(data)
                                hash_tag.append(entities["hashtags"])
                            else:
                                j = random.randrange(1, s)
                                if j < 100:

                                    tmp_list[j] = data

                                    hash_tag[j] = entities["hashtags"]
                                    N = listener.num_handled
                                    length = avg_len(tmp_list)
                                    print "The number of tweets from beginning :", N
                                    stat = top_five(hash_tag)
                                    print "Top 5 hot hashtags :"
                                    for i in range(0,len(stat)):
                                        print str(stat[i][0].encode('utf-8'))+ ":"+str(stat[i][1])
                                    print "The average length of the twitter is :", length
                                    print "\n"

        if data is None:
            listener.queue_txt.task_done()
            break

        count += 1
        s+=1
        if count == flush_every:
            sys.stdout.flush()
            count = 0
        listener.queue_txt.task_done()
        listener.queue_tags.task_done()
        listener.queue_id.task_done()


if __name__ == '__main__':
    l = SListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    t = threading.Thread(target=worker, args=(l,))
    t.start()
    stream = Stream(auth, l)

    #print_status(l)

    try:
        while True:
            try:
                stream.sample()  # sampling
            except KeyboardInterrupt:
                print 'KEYBOARD INTERRUPT', sys.stderr
                sys.exit(0)
            except (socket.error, httplib.HTTPException):
                global tcpip_delay
                print'TCP/IP Error'.format(delay=tcpip_delay,),sys.stderr

                time.sleep(min(tcpip_delay, MAX_TCPIP_TIMEOUT))
                tcpip_delay += 0.25
    finally:
        print'stream disconnect'
        stream.disconnect()


        l.queue_txt.put(None)
        l.queue_tags.put(None)
        l.queue_id.put(None)
        l.queue_txt.join()
        l.queue_tags.join()
        l.queue_id.join()
        print "thread to finish"
        t.join()
        print'Exit successful', sys.stderr

