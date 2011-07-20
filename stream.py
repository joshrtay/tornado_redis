from tornado.iostream import IOStream
from events import EventEmitter
import errno
import socket
import sys
from tornado import ioloop
from tornado import stack_context
import collections
import logging

class Stream(EventEmitter):
    def __init__(self,socket,io_loop=None,max_buffer_size=104857600,
                 read_chunk_size=4096):
        self.socket = socket
        self.socket.setblocking(False)
        self.io_loop = io_loop or ioloop.IOLoop.instance()
        self.max_buffer_size = max_buffer_size
        self.read_chunk_size = read_chunk_size
        self.writable = True
        self._write_buffer = collections.deque()
        self._write_buffer_frozen = False
        self._connecting = False
        self._state = None
        self._pending_emits = 0

    def on(self,name,fn):
        EventEmitter.on(self,name,stack_context.wrap(fn))

    def emit(self,name,*args):
        def wrapper():
            self._pending_emits -= 1
            try:
                EventEmitter.emit(self,name,*args)
            except Exception:
                logging.error("Uncaught exception, closing connection.",
                              exc_info=True)
                self.close()
                raise

            self._maybe_add_error_listener()

        with stack_context.NullContext():
            self._pending_emits += 1
            self.io_loop.add_callback(wrapper)


    def connect(self,address):
        self._connecting = True
        try:
            self.socket.connect(address)
        except socket.error,e:
            if e.args[0] not in (errno.EINPROGRESS, errno.EWOULDBLOCK): raise

        self._add_io_state(self.io_loop.WRITE)

    def write(self,data):
        self._check_closed()
        self._write_buffer.append(data)
        self._handle_write()
        if self._write_buffer:
            self._add_io_state(self.io_loop.WRITE)
        self._maybe_add_error_listener()

    def writing(self):
        return bool(self._write_buffer)

    def close(self):
        if self._state is not None:
                self.io_loop.remove_handler(self.socket.fileno())
        self.socket.close()
        self.socket = None
        self.emit("close")

    def end(self):
        self.write("")
        

    def _handle_events(self, fd, events):
        if not self.socket:
            logging.warning("Got events for closed stream %d", fd)
            return
        try:
            if events & self.io_loop.READ:
                self._handle_read()
            if not self.socket:
                return
            if events & self.io_loop.WRITE:
                if self._connecting:
                    self._handle_connect()
                self._handle_write()
            if not self.socket:
                return
            if events & self.io_loop.ERROR:
                # We may have queued up a user callback in _handle_read or
                # _handle_write, so don't close the IOStream until those
                # callbacks have had a chance to run.
                self.io_loop.add_callback(self.close)
                return
            state = self.io_loop.ERROR
            state |= self.io_loop.READ
            if self.writing():
                state |= self.io_loop.WRITE
            if state != self._state:
                assert self._state is not None, \
                    "shouldn't happen: _handle_events without self._state"
                self._state = state
                self.io_loop.update_handler(self.socket.fileno(), self._state)
        except Exception:
            logging.error("Uncaught exception, closing connection.",
                          exc_info=True)
            self.close()
            raise


    def _handle_connect(self):
        err = self.socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        if err != 0:
            # IOLoop implementations may vary: some of them return
            # an error state before the socket becomes writable, so
            # in that case a connection failure would be handled by the
            # error path in _handle_events instead of here.
            logging.warning("Connect error on fd %d: %s",
                            self.socket.fileno(), errno.errorcode[err])
            self.close()
            return
        self.emit("connect")
        self._connecting = False

    def _handle_write(self):
        while self._write_buffer:
            try:
                self.socket.send(self._write_buffer[0])
                self._write_buffer.popleft()
            except socket.error, e:
                if e.args[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
                    self._write_buffer_frozen = True
                    break
                else:
                    logging.warning("Write error on %d: %s",
                                    self.socket.fileno(), e)
                    self.close()
                    return
        if not self._write_buffer:
            self.emit("drain")
            self._add_io_state(self.io_loop.READ)
            

    def _handle_read(self):
        while True:
            try:
                result = self._read_from_socket()
            except Exception:
                self.close()
                return
            if result is None: break

    def _read_from_socket(self):
        try:
            chunk = self.socket.recv(self.read_chunk_size)
        except socket.error, e:
            if e.args[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
                return None
            else:
                raise
        if not chunk:
            self.emit("end")
            self.close()
            return None
        else:
            self.emit("data",chunk)
            return chunk

    def _check_closed(self):
        if not self.socket:
            raise IOError("Stream is closed")

    def _maybe_add_error_listener(self):
        if self._state is None and self._pending_emits == 0:
            self._add_io_state(0)

    def _add_io_state(self, state):
        """Adds `state` (IOLoop.{READ,WRITE} flags) to our event handler.

        Implementation notes: Reads and writes have a fast path and a
        slow path.  The fast path reads synchronously from socket
        buffers, while the slow path uses `_add_io_state` to schedule
        an IOLoop callback.  Note that in both cases, the callback is
        run asynchronously with `_run_callback`.

        To detect closed connections, we must have called
        `_add_io_state` at some point, but we want to delay this as
        much as possible so we don't have to set an `IOLoop.ERROR`
        listener that will be overwritten by the next slow-path
        operation.  As long as there are callbacks scheduled for
        fast-path ops, those callbacks may do more reads.
        If a sequence of fast-path ops do not end in a slow-path op,
        (e.g. for an @asynchronous long-poll request), we must add
        the error handler.  This is done in `_run_callback` and `write`
        (since the write callback is optional so we can have a
        fast-path write with no `_run_callback`)
        """
        if self.socket is None:
            # connection has been closed, so there can be no future events
            return
        if self._state is None:
            self._state = ioloop.IOLoop.ERROR | state
            with stack_context.NullContext():
                self.io_loop.add_handler(
                    self.socket.fileno(), self._handle_events, self._state)
        elif not self._state & state:
            self._state = self._state | state
            self.io_loop.update_handler(self.socket.fileno(), self._state)

    def close(self):
        if self.socket is not None:
            if self._state is not None:
                self.io_loop.remove_handler(self.socket.fileno())
            self.socket.close()
            self.socket = None
            self.emit("close")
        



            

            

    
