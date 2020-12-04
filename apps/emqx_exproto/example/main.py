#!/usr/bin/python
# -*- coding: UTF-8 -*-

from erlport import Atom
from erlport import erlang

OK = 0
ERROR = 1

##--------------------------------------------------------------------
## Connection level

def init(conn, conninfo):
    print(f'[python] established a conn={conn}, conninfo={conninfo}')

    ## set an integer num to the connection state
    ## it just a example structure to record the callback total times
    state = 0

    ## subscribe the topic `t/dn` with qos0
    subscribe(conn, b"t/dn", 0)

    ## return the initial conn's state
    return (OK, state)

def received(conn, data, state):
    print(f'[python] received data conn={conn}, data={data}, state={state}')

    ## echo the conn's data
    send(conn, data)

    ## return the new conn's state
    return (OK, state+1)

def terminated(conn, reason, state):
    print(f'[python] terminated conn={conn}, reason={reason}, state={state}')
    return

##--------------------------------------------------------------------
## Protocol/Session level

def deliver(conn, msgs, state):
    print(f'[python] received messages: conn={conn}, msgs={msgs}, state={state}')

    ## echo the protocol/session messages
    for msg in msgs:
        msg[3] = (Atom(b'topic'), b't/up')
        publish(conn, msg)

    ## return the new conn's state
    return (OK, state+1)

##--------------------------------------------------------------------
## APIs
##--------------------------------------------------------------------

def send(conn, data):
    erlang.call(Atom(b'emqx_exproto'), Atom(b'send'), [conn, data])
    return

def close(conn):
    erlang.call(Atom(b'emqx_exproto'), Atom(b'close'), [conn])
    return

def register(conn, clientinfo):
    erlang.call(Atom(b'emqx_exproto'), Atom(b'register'), [conn, clientinfo])
    return

def publish(conn, message):
    erlang.call(Atom(b'emqx_exproto'), Atom(b'publish'), [conn, message])
    return

def subscribe(conn, topic, qos):
    erlang.call(Atom(b'emqx_exproto'), Atom(b'subscribe'), [conn, topic, qos])
    return

def unsubscribe(conn, topic):
    erlang.call(Atom(b'emqx_exproto'), Atom(b'subscribe'), [conn, topic])
    return

