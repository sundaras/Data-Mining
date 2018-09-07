from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import math,sys
from collections import deque
import threading
import Queue
sc = SparkContext("local[2]", "DGIM")
ssc = StreamingContext(sc, 10)


lines = ssc.socketTextStream ("localhost" , 9999)
#lines.pprint()

N=1000
grp=2
actual_bits=Queue.Queue()
queues = []
if N == 0:
    no_buckets = -1
else:
    no_buckets= int(math.ceil(math.log(N) / math.log(2)))

queues = [deque() for _ in range(no_buckets + 1)]
time_stamp = 0
last_bucket = -1  #initial



def rdd_process(rdd):
    global N,max_index,queues,time_stamp,last_bucket

    for x in rdd.collect():
        update(x)

def update(s):
    global time_stamp,last_bucket,actual_count,actual_bits
    if actual_bits.qsize()<=1000:
        actual_bits.put(s)
    if actual_bits.qsize()>1000:
        actual_bits.get()
    if N == 0:
        return
    time_stamp = (time_stamp + 1) % (2 * N)

    if (last_bucket >= 0 and
            old_bucket(last_bucket)):
        delete_buc()
    if s == u"0":

        return

    buc_tim = time_stamp
    if last_bucket == -1:
        last_bucket = time_stamp

    for queue in queues:
        queue.appendleft(buc_tim)

        if len(queue) <= grp:
            break
        l= queue.pop()
        secondl = queue.pop()
        # merge last two buckets.
        buc_tim = secondl
        if l == last_bucket:
            last_bucket= secondl

def get_count():
    t = threading.Timer(10.0, get_count)
    t.start()

    try:


        if (actual_bits.qsize() == 1000):
            result = 0
            actual=0
            max_value = 0
            p_2 = 1
            for queue in queues:
                queue_length = len(queue)
                if queue_length > 0:
                    max_value = p_2

                    result += queue_length * p_2
                    p_2 = p_2 << 1 #increasing power of 2
            result -= math.floor(max_value / 2)

            for elem in list(actual_bits.queue):
                if elem == u"1":
                    actual+=1

            print "Estimated number of ones in last 1000 bits is:", int(result)
            print "Actual number of ones in last 1000 bits is:", int(actual)
            print "\n"
    except KeyboardInterrupt:
        print 'KEYBOARD INTERRUPT'
        t.join(10)
        t.exit()
        sys.exit()


def old_bucket(bucket_timestamp):

    return (time_stamp - bucket_timestamp) % (2 * N) >= N


def delete_buc():
    global last_bucket
    for queue in reversed(queues):
        if len(queue) > 0:
            queue.pop()
            break

    for queue in reversed(queues):
        if len(queue) > 0:
            last_bucket = queue[-1]
            break

if __name__ == '__main__':



    t = threading.Thread(target=get_count, args=())
    try:
        lines.foreachRDD(rdd_process)
        ssc.start()
        get_count()
        ssc.awaitTermination()
    except KeyboardInterrupt:
        print 'KEYBOARD INTERRUPT'
        t.join(10)
        sys.exit()


    print "Waiting for thread to finish"



    print "Exit successful"




