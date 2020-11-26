from uuid import uuid1

from pytest import fixture

from al_assignment.message import Result


@fixture
def task_id():
    return uuid1()


@fixture
def result(task_id):
    return Result(task_id)


def test_result_init(task_id):
    result = Result(task_id)
    assert result.header == 0x10
    assert result.fmt == '>B16s'
    assert result.task_id == task_id


def test_result_encode(result):
    msg = result.encode()
    assert isinstance(msg, bytes)


def test_result_decode(result, task_id):
    msg = result.encode()
    decoded_result = result.decode(msg)
    assert isinstance(decoded_result, Result)
    assert decoded_result.task_id == task_id
