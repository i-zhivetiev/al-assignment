from uuid import uuid1

from al_assignment.message import Task, decode, Result, Ping


def test_decode_task():
    task = Task()
    msg = task.encode()
    item = decode(msg)
    assert isinstance(item, Task)


def test_decode_result():
    task = Result(uuid1())
    msg = task.encode()
    item = decode(msg)
    assert isinstance(item, Result)


def test_decode_ping():
    ping = Ping()
    msg = ping.encode()
    item = decode(msg)
    assert isinstance(item, Ping)
