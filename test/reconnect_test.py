from tornado_redis.client import RedisClient,redis_print
from tornado.ioloop import IOLoop
import logging
logging.basicConfig(level=logging.DEBUG)
import time

class ReconnectTest(object):
    def __init__(self):
        redis = RedisClient()
        redis.on("error", self.on_error)
        redis.on("ready",self.on_ready)
        redis.on("reconnecting",self.on_reconnecting)
        redis.on("connect",self.on_connect)
        self.r = redis

        IOLoop.instance().add_timeout(time.time()+.2,self.now)

    def on_error(self,error=None):
        logging.info("Redis says: " + str(err))

    def on_ready(self):
        logging.info("Readis ready")

    def on_reconnecting(self,arg):
        logging.info("Redis reconnecting: "+ str(arg))

    def on_connect(self):
        logging.info("Redis connected")

    def now(self):
        self.r.send_command("set","now",time.time(),self.now_ack)

    def now_ack(self,res,error=None):
        if error:
            logging.error(str(error))
        else:
            logging.info(str(res))

if __name__ == "__main__":
    ReconnectTest()
    IOLoop.instance().start()
    
