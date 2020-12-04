#!/usr/bin/python
# -*- coding: UTF-8 -*-

OK = 0
ERROR = 1

## Return :: (HookSpec, State)
##
## HookSpec :: [(HookName, CallbackModule, CallbackFunction, Opts)]
## State  :: Any
##
## HookName :: "client_connect" | "client_connack" | "client_connected" | ...
## CallbackModule :: ...
## CallbackFunctiin :: ...
## Opts :: [(Key, Value)]
def init():
    ## Maybe a connection object?
    state = ()
    hookspec = [("client_connect",      "main", "on_client_connect", []),
                ("client_connack",      "main", "on_client_connack", []),
                ("client_connected",    "main", "on_client_connected", []),
                ("client_disconnected", "main", "on_client_disconnected", []),
                ("client_authenticate", "main", "on_client_authenticate", []),
                ("client_check_acl",    "main", "on_client_check_acl", []),
                ("client_subscribe",    "main", "on_client_subscribe", []),
                ("client_unsubscribe",  "main", "on_client_unsubscribe", []),
                ("session_created",     "main", "on_session_created", []),
                ("session_subscribed",  "main", "on_session_subscribed", []),
                ("session_unsubscribed","main", "on_session_unsubscribed", []),
                ("session_resumed",     "main", "on_session_resumed", []),
                ("session_discarded",   "main", "on_session_discarded", []),
                ("session_takeovered",  "main", "on_session_takeovered", []),
                ("session_terminated",  "main", "on_session_terminated", []),
                ("message_publish",     "main", "on_message_publish", [("topics", ["t/#"])]),
                ("message_delivered",   "main", "on_message_delivered", [("topics", ["t/#"])]),
                ("message_acked",       "main", "on_message_acked", [("topics", ["t/#"])]),
                ("message_dropped",     "main", "on_message_dropped", [("topics", ["t/#"])])
               ]
    return (OK, (hookspec, state))

def deinit():
    return

##--------------------------------------------------------------------
## Callback functions
##--------------------------------------------------------------------


##--------------------------------------------------------------------
## Clients

def on_client_connect(conninfo, props, state):
    print("on_client_connect: conninfo: {0}, props: {1}, state: {2}".format(conninfo, props, state))
    return

def on_client_connack(conninfo, rc, props, state):
    print("on_client_connack: conninfo: {0}, rc{1}, props: {2}, state: {3}".format(conninfo, rc, props, state))
    return

def on_client_connected(clientinfo, state):
    print("on_client_connected: clientinfo: {0}, state: {1}".format(clientinfo, state))
    return

def on_client_disconnected(clientinfo, reason, state):
    print("on_client_disconnected: clientinfo: {0}, reason: {1}, state: {2}".format(clientinfo, reason, state))
    return

def on_client_authenticate(clientinfo, authresult, state):
    print("on_client_authenticate: clientinfo: {0}, authresult: {1}, state: {2}".format(clientinfo, authresult, state))
    ## True / False
    return (OK, True)

def on_client_check_acl(clientinfo, pubsub, topic, result, state):
    print("on_client_check_acl: clientinfo: {0}, pubsub: {1}, topic: {2}, result: {3}, state: {4}".format(clientinfo, pubsub, topic, result, state))
    ## True / False
    return (OK, True)

def on_client_subscribe(clientinfo, props, topics, state):
    print("on_client_subscribe: clientinfo: {0}, props: {1}, topics: {2}, state: {3}".format(clientinfo, props, topics, state))
    return

def on_client_unsubscribe(clientinfo, props, topics, state):
    print("on_client_unsubscribe: clientinfo: {0}, props: {1}, topics: {2}, state: {3}".format(clientinfo, props, topics, state))
    return

##--------------------------------------------------------------------
## Sessions

def on_session_created(clientinfo, state):
    print("on_session_created: clientinfo: {0}, state: {1}".format(clientinfo, state))
    return

def on_session_subscribed(clientinfo, topic, opts, state):
    print("on_session_subscribed: clientinfo: {0}, topic: {1}, opts: {2}, state: {3}".format(clientinfo, topic, opts, state))
    return

def on_session_unsubscribed(clientinfo, topic, state):
    print("on_session_unsubscribed: clientinfo: {0}, topic: {1}, state: {2}".format(clientinfo, topic, state))
    return

def on_session_resumed(clientinfo, state):
    print("on_session_resumed: clientinfo: {0}, state: {1}".format(clientinfo, state))
    return

def on_session_discarded(clientinfo, state):
    print("on_session_discared: clientinfo: {0}, state: {1}".format(clientinfo, state))
    return

def on_session_takeovered(clientinfo, state):
    print("on_session_takeovered: clientinfo: {0}, state: {1}".format(clientinfo, state))
    return

def on_session_terminated(clientinfo, reason, state):
    print("on_session_terminated: clientinfo: {0}, reason: {1}, state: {2}".format(clientinfo, reason, state))
    return

##--------------------------------------------------------------------
## Messages

def on_message_publish(message, state):
    print("on_message_publish: message: {0}, state: {1}".format(message, state))
    return message

def on_message_dropped(message, reason, state):
    print("on_message_dropped: message: {0}, reason: {1}, state: {2}".format(message, reason, state))
    return

def on_message_delivered(clientinfo, message, state):
    print("on_message_delivered: clientinfo: {0}, message: {1}, state: {2}".format(clientinfo, message, state))
    return

def on_message_acked(clientinfo, message, state):
    print("on_message_acked: clientinfo: {0}, message: {1}, state: {2}".format(clientinfo, message, state))
    return
