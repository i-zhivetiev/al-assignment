# coding: utf-8
import struct
import uuid


class Message(object):
    header = None
    fmt = None

    def encode(self):
        raise NotImplementedError

    @classmethod
    def decode(cls, msg):
        raise NotImplementedError


class MessageError(Exception):
    """
    """


class MessageDecodeError(MessageError):
    """
    """


class Task(Message):
    """
    >>> task = Task()
    >>> msg = task.encode()
    >>> decoded_task = Task.decode(msg)
    >>> isinstance(decoded_task, Task)
    True
    >>> task.task_id == decoded_task.task_id
    True
    """
    header = 0x01
    fmt = '>B16s'

    def __init__(self, task_id=None):
        if task_id is None:
            self.task_id = uuid.uuid1()
        else:
            self.task_id = task_id

    def encode(self):
        return struct.pack(self.fmt, self.header, self.task_id.bytes)

    @classmethod
    def decode(cls, msg):
        header, task_id = struct.unpack(cls.fmt, msg)
        task_id = uuid.UUID(bytes=task_id)
        if header != cls.header:
            raise MessageDecodeError('Unknown header: %s' % header)
        return cls(task_id)


class Result(Task):
    """
    >>> task = Task()
    >>> result = Result(task.task_id)
    >>> msg = result.encode()
    >>> decoded_result = Result.decode(msg)
    >>> isinstance(decoded_result, Result)
    True
    >>> decoded_result.task_id == task.task_id
    True
    """
    header = 0x10

    def __init__(self, task_id):
        super(Result, self).__init__(task_id)


class Ping(Message):
    """
    >>> ping = Ping()
    >>> msg = ping.encode()
    >>> decoded_ping = Ping.decode(msg)
    >>> isinstance(decoded_ping, Ping)
    True
    """
    header = 0x11
    fmt = '>B'

    def encode(self):
        return struct.pack(self.fmt, self.header)

    @classmethod
    def decode(cls, msg):
        (header,) = struct.unpack(cls.fmt, msg)
        if header != cls.header:
            raise MessageDecodeError('Unknown header: %s' % header)
        return cls()


# TODO: сделать заполнение автоматом с помощью Abs
KNOWN_HEADERS = {
    0x01: Task,
    0x10: Result,
    0x11: Ping,
}


def decode(msg):
    """
    >>> task = Task()
    >>> msg = task.encode()
    >>> item = decode(msg)
    >>> isinstance(item, Task)
    True
    >>> result = Result(task.task_id)
    >>> msg = result.encode()
    >>> item = decode(msg)
    >>> isinstance(item, Result)
    True
    >>> ping = Ping()
    >>> msg = ping.encode()
    >>> item = decode(msg)
    >>> isinstance(item, Ping)
    True
    """
    (header,) = struct.unpack('>B', msg[0])
    if header not in KNOWN_HEADERS:
        raise MessageDecodeError("Unknown message")
    decoder = KNOWN_HEADERS[header]
    return decoder.decode(msg)
