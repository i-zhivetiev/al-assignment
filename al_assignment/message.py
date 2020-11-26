# coding: utf-8
"""
Модуль содержит классы и инструменты, для работы
с сообщениями, которыми обмениваются процессы.
"""
import struct
import uuid

LEN = 17


class Message(object):
    """Базовый класс сообщения."""
    header = None
    fmt = None

    __slots__ = []

    def encode(self):
        raise NotImplementedError

    @classmethod
    def decode(cls, msg):
        raise NotImplementedError


class MessageError(Exception):
    """Базовый класс ошибок, связанных с Message."""


class MessageDecodeError(MessageError):
    """Ошибка декодирования сообщения."""


class Task(Message):
    """Сообщение с задачей на вычисление."""
    header = 0x01
    fmt = '>B16s'

    __slots__ = ['task_id']

    def __init__(self, task_id=None):
        """Создать экземпляр Задачи.

        :param task_id:
            `uuid`|None, идентификатор задачи, по умолчанию генерируется UUID1.

        :returns:
            `Task`, экземпляр задачи.
        """
        if task_id is None:
            self.task_id = uuid.uuid1()
        else:
            self.task_id = task_id

    def encode(self):
        """Закодировать задачу в двоичную строку, пригодную для передачи
         и вернуть результат.

        :returns:
            `bytes`, закодированное сообщение.
        """
        return struct.pack(self.fmt, self.header, self.task_id.bytes)

    @classmethod
    def decode(cls, msg):
        """Декодировать сообщение `msg` и вернуть экземпляр `Task`.

        :param msg:
            `bytes`, двоичная строка с сообщением.

        :returns:
            `Task`, экземпляр задачи.
        """
        header, task_id = struct.unpack(cls.fmt, msg)
        task_id = uuid.UUID(bytes=task_id)
        if header != cls.header:
            raise MessageDecodeError('Unknown header: %s' % header)
        return cls(task_id)


class Result(Task):
    """Сообщение с результатом выполнения задачи.

    Идентичен родительскому классу с одним отличием:
    параметр конструктора `task_id` является обязательным.
    """
    header = 0x10

    __slots__ = ['task_id']

    def __init__(self, task_id):
        """Создать экземпляр Result.

        :param task_id:
            `uuid`, идентификатор задачи, с которым связан результат.

        :returns:
            `Result`
        """
        super(Result, self).__init__(task_id)


class Ping(Message):
    """Сообщение для авто-теста."""
    header = 0x11
    fmt = '>B'

    __slots__ = []

    def encode(self):
        """Закодировать пинг в двоичную строку, пригодную для передачи
        и вернуть результат.

        :returns:
            `bytes`, закодированное сообщение.
        """
        return struct.pack(self.fmt, self.header)

    @classmethod
    def decode(cls, msg):
        """Декодировать сообщение `msg` и вернуть экземпляр `Ping`.

        :param msg:
            `bytes`, двоичная строка с сообщением.

        :returns:
            `Ping`, экземпляр.
        """
        (header,) = struct.unpack(cls.fmt, msg)
        if header != cls.header:
            raise MessageDecodeError('Unknown header: %s' % header)
        return cls()


KNOWN_HEADERS = {
    0x01: Task,
    0x10: Result,
    0x11: Ping,
}


def decode(msg):
    """Декодировать сообщение `msg` и вернуть экземпляр соответствующего
    класса.

    :param msg:
        `bytes`, двоичная строка.

    :returns:
        `Message`, соответствующий наследник Message.
    """
    header = msg[0]
    if not isinstance(header, int):
        (header,) = struct.unpack('>B', header)
    if header not in KNOWN_HEADERS:
        raise MessageDecodeError("Unknown message")
    decoder = KNOWN_HEADERS[header]
    return decoder.decode(msg)


def is_ping(msg):
    return isinstance(msg, Ping)


def is_result(msg):
    return isinstance(msg, Result)


def is_task(msg):
    return isinstance(msg, Task)
