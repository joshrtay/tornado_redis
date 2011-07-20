from tornado.testing import AsyncHTTPTestCase
from tornado.web import RequestHandler,Application
import unittest

import socket
import sys
sys.path.append('../')
from stream import Stream

class HelloHandler(RequestHandler):
    def get(self):
        self.write("Hello")

class StreamTestCase(AsyncHTTPTestCase):
    def get_app(self):
        return Application([('/',HelloHandler)])

    def test_read(self):
        s = socket.socket(socket.AF_INET,socket.SOCK_STREAM,0)
        s.connect(("localhost",self.get_http_port()))
        self.stream = Stream(s,io_loop=self.io_loop)
        self.stream.write(b"GET / HTTP/1.0\r\n\r\n")
        self.stream.on("data",self.on_data1)
        self.wait()

    def on_data1(self,data):
        self.assertEquals(data.split("\r\n\r\n")[-1],"Hello")
        self.stop()

if __name__ == '__main__':
    unittest.main()
    
