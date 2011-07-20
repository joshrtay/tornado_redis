from tornado_redis.client import RedisClient,redis_print
from tornado.ioloop import IOLoop
import logging
logging.basicConfig(level=logging.DEBUG)

def log_error(err):
    logging.info("Error: " + str(err))

class SimpleTest(object):
    def __init__(self):
        c = RedisClient()
        self.c = c
        c.on("error",log_error)
        c.set("string key","string val", redis_print)
        c.hset("hash key", "hashtest 1", "some value", redis_print)
        c.hset("hash key", "hashtest 2", "some other value",redis_print)
        c.hkeys("hash key",self.hkeys_ack)

    def hkeys_ack(self,replies,error=None):
        if error:
            print error
            return
        print "%d replies: " % (len(replies))
        for i,reply in enumerate(replies):
            print "    %d: %s" % (i,reply)
        #self.c.send_command("quit",None)
        
if __name__ == "__main__":
    s = SimpleTest()
    IOLoop.instance().start()
