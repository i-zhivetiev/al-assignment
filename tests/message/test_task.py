from uuid import uuid1, UUID

from pytest import fixture

from al_assignment.message import Task


@fixture
def task_id():
    return uuid1()


@fixture
def task(task_id):
    return Task(task_id)


def test_task_init():
    task = Task()
    assert isinstance(task.task_id, UUID)
    assert task.header == 0x01
    assert task.fmt == '>B16s'


def test_task_init_task_id():
    task_id = uuid1()
    task = Task(task_id)
    assert task.task_id == task_id


def test_task_encode(task):
    msg = task.encode()
    assert isinstance(msg, bytes)


def test_task_decode(task, task_id):
    msg = task.encode()
    decoded_task = task.decode(msg)
    assert isinstance(decoded_task, Task)
    assert decoded_task.task_id == task_id
