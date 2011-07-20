class EventEmitter(object):
    def on(self,name,fn):
        if not hasattr(self,'_events'):
            self._events = {}

        if name not in self._events:
            self._events[name] = fn
        elif isinstance(self._events[name],list):
            self._events[name].append(fn)
        else:
            self._events[name] = [self._events[name],fn]

    def addListener(self,*args):
        self.on(*args)

    def once(self,name,fn):
        def fn_w(*args):
            fn(*args)
            self.removeListener(name,fn_w)
        self.on(name,fn_w)

    def removeListener(self,name,fn):
        if hasattr(self,'_events') and name in self._events:
            ls = self._events[name]
            ls.remove(name)
            if not ls:
                self._events.pop(name)

    def removeAllListeners(self,name):
        if hasattr(self,'_events') and name in self._events:
            self._events.pop(name)

    def listeners(self,name):
        if not hasattr(self,'_events'):
            self._events = {}

        if name not in self._events:
            self._events[name] = []

        if not isinstance(self._events[name],list):
            self._events[name] = [self._events[name]]

        return self._events[name]

    def emit(self,name,*args):
        if not self._events:
            return False

        if name not in self._events:
            return False

        handler = self._events[name]
        if isinstance(handler,list):
            for listener in handler:
                listener(*args)
        else:
            handler(*args)

        return True
        
        
