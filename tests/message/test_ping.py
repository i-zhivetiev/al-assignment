from al_assignment.message import Ping


def test_ping_init():
    ping = Ping()
    assert ping.header == 0x11
    assert ping.fmt == '>B'


def test_ping_encode():
    ping = Ping()
    msg = ping.encode()
    assert isinstance(msg, bytes)


def test_ping_decode():
    ping = Ping()
    msg = ping.encode()
    decoded_ping = ping.decode(msg)
    assert isinstance(decoded_ping, Ping)
