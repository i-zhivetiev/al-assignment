# coding: utf-8
from random import sample

from pytest import fixture, mark

from al_assignment.helpers.waiting_list import WaitingList


@fixture
def waiting_list():
    return WaitingList()


@fixture
def make_expected_list(waiting_list):
    def inner(item_count):
        expected = []
        for i in range(item_count):
            expected.append((i, 'item-%s' % i))
        for p, i in sample(expected, len(expected)):
            waiting_list.add(item=i, priority=p)
        return expected

    return inner


@mark.parametrize('priority', [-1, 0, 1])
def test_add(priority, waiting_list):
    item = 'hello'
    entry = [priority, 0, item]
    waiting_list.add(item, priority=priority)
    assert waiting_list._queue == [entry]
    assert waiting_list._entries == {item: entry}


def test_pop(waiting_list, make_expected_list):
    item_count = 7
    expected = make_expected_list(item_count)
    result = [waiting_list.pop() for _ in range(item_count)]
    assert result == expected


def test_pop_item(waiting_list, make_expected_list):
    item_count = 5
    expected = make_expected_list(5)

    result = waiting_list.pop(item='item-1')
    assert result == (1, 'item-1')

    expected.remove((1, 'item-1'))
    result = []
    for i in range(item_count - 1):
        result.append(waiting_list.pop())
    assert result == expected


def test_drop(waiting_list, make_expected_list):
    item_count = 3
    expected = make_expected_list(item_count)
    waiting_list.drop('item-1')
    expected.remove((1, 'item-1'))
    result = [waiting_list.pop() for _ in range(item_count - 1)]
    assert result == expected


def test_get_top(waiting_list, make_expected_list):
    item_count = 8
    expected = make_expected_list(item_count)
    assert waiting_list.top() == (0, 'item-0')
    result = [waiting_list.pop() for _ in range(item_count)]
    assert result == expected


def test_get_top_on_removed(waiting_list, make_expected_list):
    waiting_list.add('item-0')
    waiting_list.add('item-1')
    waiting_list.drop('item-0')
    assert waiting_list.top() == (0, 'item-1')


def test_get_top_on_empty_queue(waiting_list):
    assert waiting_list.top() is None
    waiting_list.add('item', 1)
    waiting_list.drop('item')
    assert waiting_list.top() is None
