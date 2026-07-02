#!/usr/bin/env python3
"""Long-lived MQTT subscriber for the cluster smoke test.

Connects through HAProxy, subscribes to the smoke topic and prints a line for
every message received. Runs until killed. Used together with publisher.py and
run.sh to verify cross-node message routing in a two-node cluster.
"""
import sys
import time
import paho.mqtt.client as mqtt

HOST = sys.argv[1] if len(sys.argv) > 1 else "haproxy"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 1883
TOPIC = sys.argv[3] if len(sys.argv) > 3 else "cluster/smoke/1"


def new_client(cid):
    try:  # paho-mqtt 2.x
        return mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION1, client_id=cid, protocol=mqtt.MQTTv5
        )
    except AttributeError:  # paho-mqtt 1.x
        return mqtt.Client(client_id=cid, protocol=mqtt.MQTTv5)


def on_connect(cl, u, flags, rc, props=None):
    print(f"SUBSCRIBER CONNECTED rc={getattr(rc, 'value', rc)}", flush=True)
    cl.subscribe(TOPIC, 1)


def on_message(cl, u, msg):
    print(f"RECEIVED {msg.topic} {msg.payload.decode(errors='replace')}", flush=True)


c = new_client("cluster-smoke-sub")
c.on_connect = on_connect
c.on_message = on_message
c.connect(HOST, PORT, 30)
c.loop_start()
while True:
    time.sleep(1)
