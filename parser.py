import hiredis
from events import EventEmitter
from Jso import Jso

class Parser(EventEmitter):
    def __init__(self,**options):
        self.name = "hiredis"
        self.options = Jso(options)
        self.reset()


    def reset(self):
        self.reader = hiredis.Reader()

    def execute(self,data):
        self.reader.feed(data)
        try:
            while True:
                reply = self.reader.gets()
                if reply is None or reply is False: break

                if isinstance(reply,hiredis.ReplyError):
                    self.emit("reply error",reply)
                else:
                    self.emit("reply",reply)

        except Exception,e:
            self.emit("error",e)
                
        
