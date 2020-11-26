from uuid import UUID

from pytest import fixture, raises

from al_assignment.dispatcher import ResultTracker, AwaitedResult
from al_assignment.message import Task


@fixture
def result_tracker():
    return ResultTracker()


@fixture
def task_id():
    return UUID(int=1)


@fixture
def task(task_id):
    return Task(task_id)


@fixture
def awaited_task(task):
    return AwaitedResult(('client-ip', 1000), ('worker-ip', 2000), task)


def test_add(result_tracker, awaited_task, task_id):
    result_tracker.add(awaited_task)
    assert result_tracker._result_queue.pop() == (0, task_id)
    assert result_tracker._tasks.pop(task_id) == awaited_task


def test_pop(result_tracker, awaited_task, task_id):
    result_tracker.add(awaited_task)
    assert result_tracker.pop(task_id) == (0, awaited_task)
    assert len(result_tracker._tasks) == 0
    with raises(KeyError):
        result_tracker._result_queue.pop()


def test_top(result_tracker, awaited_task, task_id):
    assert result_tracker.top() is None
    result_tracker.add(awaited_task)
    assert result_tracker.top() == (0, awaited_task)
    assert result_tracker.pop(task_id) == (0, awaited_task)
