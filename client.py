from parser import Parser
from events import EventEmitter
from stream import Stream
from Jso import Jso
import collections
import operator
import socket
import functools
from itertools import imap
import logging
from tornado.ioloop import IOLoop
import time
import sys
import traceback

COMMANDS = set(["get", "set", "setnx", "setex", "append", "strlen", "del", "exists", "setbit", "getbit", "setrange", "getrange", "substr",
    "incr", "decr", "mget", "rpush", "lpush", "rpushx", "lpushx", "linsert", "rpop", "lpop", "brpop", "brpoplpush", "blpop", "llen", "lindex",
    "lset", "lrange", "ltrim", "lrem", "rpoplpush", "sadd", "srem", "smove", "sismember", "scard", "spop", "srandmember", "sinter", "sinterstore",
    "sunion", "sunionstore", "sdiff", "sdiffstore", "smembers", "zadd", "zincrby", "zrem", "zremrangebyscore", "zremrangebyrank", "zunionstore",
    "zinterstore", "zrange", "zrangebyscore", "zrevrangebyscore", "zcount", "zrevrange", "zcard", "zscore", "zrank", "zrevrank", "hset", "hsetnx",
    "hget", "hmset", "hmget", "hincrby", "hdel", "hlen", "hkeys", "hvals", "hgetall", "hexists", "incrby", "decrby", "getset", "mset", "msetnx",
    "randomkey", "select", "move", "rename", "renamenx", "expire", "expireat", "keys", "dbsize", "auth", "ping", "echo", "save", "bgsave",
    "bgrewriteaof", "shutdown", "lastsave", "type", "multi", "exec", "discard", "sync", "flushdb", "flushall", "sort", "info", "monitor", "ttl",
    "persist", "slaveof", "debug", "config", "subscribe", "unsubscribe", "psubscribe", "punsubscribe", "publish", "watch", "unwatch", "cluster",
    "restore", "migrate", "dump", "object", "client", "eval", "evalsha"])

class Command(object):
    def __init__(self,*args):
        self.command = args[0]
        self.args = args[1]
        if len(args) > 2:
            self.sub_command = args[2]
        else:
            self.sub_command = None

        if len(args) > 3:
            self.callback = args[3]
        else:
            self.callback = None

    def call_callback(self,reply,error):
        try:
            self.callback(reply,error=error)
        except Exception:
            logging.error("Uncaught exceptions in command callback.",
                          exc_info=True)
            
    
class ReplyParserError(Exception):
    pass

class DeferredException(Exception):
    def __init__(self,e,exc_info):
        self.e = e
        self.exc_info = exc_info

    def __str__(self):
        return traceback.format_exc(self.exc_info)
        
            
class RedisClient(EventEmitter):
    def __init__(self,*args,**options):
        self.host = "127.0.0.1" if len(args) < 2 else args[1]
        self.port = 6379 if len(args) < 1 else args[0]
        
        sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM,0)

        self.options = Jso(options)
        self.stream = Stream(sock,io_loop=self.options.io_loop)
        self.stream.connect((self.host,self.port))


        self.connected = False
        self.ready = False
        self.send_anyway = True
        self.connections = 0
        self.attempts = 1
        self.command_queue = collections.deque()
        self.offline_queue = collections.deque()
        self.commands_sent = 0
        self.retry_delay = .25
        self.retry_timer = None
        self.emitted_end = False
        self.current_retry_delay = self.retry_delay
        self.retry_backoff = 1.7
        self.subscriptions = False
        self.monitoring = False
        self.closing = False
        self.server_info = Jso()
        self.auth_pass = None
        self.encoding = self.options.encoding or 'utf-8'
        self.encoding_error = self.options.encoding_error or 'strict'

        self.reply_parser = Parser()
        self.reply_parser.on("reply_error",self.return_error)
        self.reply_parser.on("reply",self.return_reply)
        self.reply_parser.on("error",self.return_error_unrecoverable)

        self.stream.on("connect",self.on_connect)
        self.stream.on("data",self.on_data)
        #TODO: create error event
        #self.stream.on("error",self.on_error)
        self.stream.on("close",functools.partial(self.connection_gone,"close"))
        self.stream.on("end",functools.partial(self.connection_gone,"end"))
        

    #### Parser Callbacks ####
        
        
    def return_error_unrecoverable(self,err):
        self.emit("error",ReplyParserError(str(err)))

    def return_error(self,err):
        command_obj = self.command_queue.popleft()

        if not self.subscriptions and len(self.command_queue) == 0:
            self.emit("idle")

        if command_obj and operator.isCallable(command_obj.callback):
            command_obj.call_callback(None,err)
        else:
            logging.debug("tornado-redis: no callback to send error: %s" % str(err))
            #IOLoop.instance().add_callback(functools.partial(self.raise_error,err))


    def return_reply(self,reply):
        command_obj = self.command_queue.popleft() if len(self.command_queue) > 0 else None

        if not self.subscriptions and len(self.command_queue) == 0:
            self.emit("idle")

        if command_obj and not command_obj.sub_command:
            if operator.isCallable(command_obj.callback):
                if reply and command_obj.command.lower() == 'hgetall':
                    i = 0
                    obj = Jso()
                    while i < len(reply):
                        key = str(reply[i])
                        val = reply[i+1]
                        obj[key] = val
                    reply = obj

                command_obj.call_callback(reply,None)
            else:
                logging.debug("no callback for reply: %s" % reply)

        elif self.subscriptions or (command_obj and command_obj.sub_command):
            if isinstance(reply,list):
                if reply[0] in ["subscribe","unsubscribe","psubscribe"] and reply[2] == 0:
                    self.subscriptions = False
                    logging.debug("All subscriptions removed, exiting pub/sub mode")
                if reply[0] not in ["message","pmessage","subscribe","unsubscribe","psubscribe"]:
                    raise TypeError("subscriptions are active but unknow reply type %s" % reply[0])

                try:
                    self.emit(*reply)
                except Exception:
                     logging.error("Uncaught exceptions in subscriptions.",
                                   exc_info=True)
            elif not self.closing:
                raise ValueError("subscriptions are active but got an invalid reply: %s" % str(reply))
        elif self.monitoring:
            l = reply.index(" ")
            timestamp = replice[:l]
            finds = re.finditer('[^"]+',reply[l:])
            args = []
            for find in finds:
                args.append(find.replace('"',''))
            self.emit("monitor",timestamp,args)
        else:
            raise ValueError("tornado-redis command queue state error. If you can reproduce this, please report it")


    #### Stream Callbacks ####

    def on_connect(self):
        logging.debug("Stream connected %s:%d fd %s" % (self.host,self.port,self.stream.socket.fileno()))

        self.connected = True
        self.ready = False
        self.attempts = 0
        self.connections += 1


        self.current_retry_delay = self.retry_timer
        #TODO: implement stream retry delay

        if self.auth_pass:
            self.do_auth()
        else:
            self.emit("connect")
            if self.options.no_ready_check:
                self.ready = True
                self.send_offline_queue()
            else:
                self.ready_check()


    def on_data(self,data):
        logging.debug("net read %s:%d fd %s %s" % (self.host,self.port,self.stream.socket.fileno(),str(data)))
        try:
            self.reply_parser.execute(data)
        except Exception,e:
            self.emit("error",e)

    def connection_gone(self,why):
        if self.retry_timer: return

        self.stream.close()

        logging.debug("Redis connection is gone from " + why + " event.")

        self.connected = False
        self.ready = False
        self.subscriptions = False
        self.monitoring = False

        if not self.emitted_end:
            self.emit("end")
            self.emitted_end = True

        while self.command_queue:
            command = self.command_queue.popleft()
            if operator.isCallable(command.callback):
                command.callback(None,error="Server connection closed")

        if self.closing:
            self.retry_time = None
            return

        self.current_retry_delay = self.retry_delay * self.retry_backoff
        logging.debug("Retry connection in " + str(self.current_retry_delay) + " ms")
        self.attempts += 1
        self.emit("reconnecting",Jso({'delay': self.current_retry_delay,'attempt': self.attempts}))
        self.retry_time = IOLoop.instance().add_timeout(
            time.time()+self.current_retry_delay,self.reconnect)

    def reconnect(self):
        self.stream.connect((self.host,self.port))
        self.retry_time = None

    #### Helpers #####

    def auth(self,*args):
        self.auth_pass = args[0]
        self.auth_callback = args[1]
        logging.debug("Saving auth as " + self.auth_pass)
        if self.connected:
            self.send_command("auth",args)
            
    def do_auth(self):
        logging.debug("Sending auth to %s:%s fd %s" % (self.host,str(self.port),self.stream.socket.fileno()))
        self.send_anyway = True
        self.send_command("auth", [self.auth_pass],self.auth_ack)
        self.send_anyway = False

    def auth_ack(self,err,res):
        if err:
            if str(err).findall("LOADING"):
                logging.info("Redis still loading, trying to authenticate later")
                IOLoop.instance().add_timeout(time.time()+2,self.do_auth)
            else:
                self.emit("error", "Auth failed: " + str(res))

        if str(res) != "OK":
            return self.emit("error", "Auth failed: " + str(res))


        logging.debug("Auth succeeded %s:%s fd %s" % (self.host,self.port,self.stream.socket.fileno()))
        if self.auth_callback:
            self.auth_callback(res,error=err)
            self.auth_callback = None

        self.emit("connect")
        if self.options.no_ready_check:
            self.ready = True
            self.send_offline_queue()
        else:
            self.ready_check()

    def ready_check(self):
        logging.debug("checking server ready state...")
        self.send_anyway = True
        self.send_command("info",self.info_ack)
        self.send_anyway = False

    def info_ack(self,res,error=None):
        if (error):
            return self.emit("error", "Ready check failed: " + err)

        lines = str(res).split("\r\n")
        obj = Jso()
        for line in lines:
            parts = line.split(":")
            if len(parts) == 2:
                obj[parts[0]] = parts[1]

        obj.versions = []
        for num in obj.redis_version.split("."):
            obj.versions.append(float(num))

        self.server_info = obj

        if not obj.loading or not (obj.loading and obj.loading == "0"):
            logging.debug("Redis server ready")

            self.ready = True
            self.send_offline_queue()
            self.emit("ready")
        else:
            retry_time = obj.loading_eta_seconds
            if retry_time > 1:
                retry_time = 1
            logging.debug("Redis server still loading, try again in " + retry_time)
            IOLoop.instance().add_timeout(time.time()+retry_time, self.ready_check)

    def send_offline_queue(self):
        while self.offline_queue:
            command_obj = self.offline_queue.popleft()
            logging.debug("Sending offline command: " + command_obj.command)
            self.send_command(command_obj.command,command_obj.args,command_obj.callback)




    #### Send Command ####

    def __getattr__(self,name):
        if name in COMMANDS:
            return functools.partial(self.send_command,name)
        else:
            raise AttributeError(name)

    def send_command(self,command, *args):
        stream = self.stream

        if not isinstance(command,str):
            raise TypeError("First argument to send_command must be the command name string, not " + type(command))

        args = list(args)
        if len(args) > 0 and operator.isCallable(args[-1]):
            callback = args.pop(-1)
        else:
            callback = None

        command_obj = Command(command,args,False,callback)

        if (not self.ready and not self.send_anyway) or not stream.writable:
            if not stream.writable:
                logging.debug("send command: stream is not writeable")
            logging.debug("Queueing " + command + " for next server connection.")
            self.offline_queue.append(command_obj)
            return False

        if command in ["subscribe","psubscribe","unsubscribe","punsubscribe"]:
            if not self.subscriptions:
                logging.debug("Entering pub/sub mode from " + command)
            command_obj.sub_command = True
            self.subscriptions = True
        elif command == "monitoring": self.monitoring = True
        elif command == "quit": self.closing = True
        elif self.subscriptions:
            raise ValueError("Connection in pub/sub mode, only pub/sub commands may be used")
        self.command_queue.append(command_obj)
        self.commands_sent += 1

        args.insert(0,command)
        command = ['$%s\r\n%s\r\n' % (len(enc_value), enc_value)
                   for enc_value in imap(self.encode, args)]
        command_str = '*%s\r\n%s' % (len(command), ''.join(command))
        logging.debug("send %s:%s fd %s: %s" % (self.host,self.port,self.stream.socket.fileno(), command_str))
        stream.write(command_str)

    def encode(self,value):
        if isinstance(value, unicode):
            return value.encode(self.encoding, self.encoding_errors)
        return str(value)

    def end(self):
        self.stream._events = {}
        self.connected = False
        self.ready = False
        self.stream.end()


def redis_print(reply,error=None):
    if (error):
        logging.info("Error: " + error)
    else:
        logging.info("Reply: " + str(reply))

    



            
            
            
            

        
                  
    
                                                      

        
                        
