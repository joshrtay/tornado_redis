from tornado_redis.client import RedisClient,redis_print
from tornado.ioloop import IOLoop
import logging


class PubSubExample(object):
    def __init__(self):
        c1 = RedisClient()
        self.c1 = c1
        self.c2  = RedisClient()
        self.msg_count = 0
        c1.on("subscribe",self.c1_subscribe_ack)
        c1.on("unsubscribe",self.c1_unsubscribe_ack)
        c1.on("message",self.c1_message_ack)
        c1.on("ready",self.c1_ready_ack)
        self.c2.on("ready",self.c2_ready_ack)

    def c1_subscribe_ack(self,channel,count):
        print "client1 subscribed to channel " + channel + ", " + str(count) + " total subscriptions"
        if count == 2:
            self.c2.publish("a nice channel","I am sending a message.")
            self.c2.publish("another one","I am sending a second message.")
            self.c2.publish("a nice channel","I am sending my last message.")

    def c1_unsubscribe_ack(self,channel,count):
        print "client1 unsubscribe from " + channel + ", " + str(count) + " total subscriptions"
        if count == 0:
            self.c2.end()
            self.c1.end()

    def c1_message_ack(self,channel,message):
        print "client1 channel " + channel + ": " + message
        self.msg_count += 1
        if self.msg_count == 3:
            self.c1.unsubscribe()

    def c1_ready_ack(self):
        print "c1 ready"
        self.c1.incr("did a thing")
        self.c1.subscribe("a nice channel", "another one")

    def c2_ready_ack(self):
        pass
        
if __name__ == "__main__":
    s = PubSubExample()
    IOLoop.instance().start()
