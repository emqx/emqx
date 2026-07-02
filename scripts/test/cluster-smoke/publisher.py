#!/usr/bin/env python3
"""One-shot MQTT publisher for the cluster smoke test.

Connects through HAProxy (a separate connection from the subscriber, so with
HAProxy's static round-robin balancing it typically lands on the other node)
and publishes a payload to the smoke topic.
"""
import sys
import time
import paho.mqtt.client as mqtt

HOST = sys.argv[1] if len(sys.argv) > 1 else "haproxy"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 1883
TOPIC = sys.argv[3] if len(sys.argv) > 3 else "cluster/smoke/1"
PAYLOAD = sys.argv[4] if len(sys.argv) > 4 else "cluster-smoke-payload"


def new_client(cid):
    try:  # paho-mqtt 2.x
        return mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION1, client_id=cid, protocol=mqtt.MQTTv5
        )
    except AttributeError:  # paho-mqtt 1.x
        return mqtt.Client(client_id=cid, protocol=mqtt.MQTTv5)


c = new_client("cluster-smoke-pub")
connected = {}
c.on_connect = lambda cl, u, flags, rc, props=None: connected.update(
    rc=getattr(rc, "value", rc)
)
c.connect(HOST, PORT, 30)
c.loop_start()
for _ in range(50):
    if "rc" in connected:
        break
    time.sleep(0.1)
if connected.get("rc") != 0:
    print(f"PUBLISHER CONNECT FAILED rc={connected.get('rc')}", flush=True)
    sys.exit(1)
info = c.publish(TOPIC, PAYLOAD.encode(), qos=1)
info.wait_for_publish(10)
print(f"PUBLISHED {TOPIC} {PAYLOAD}", flush=True)
c.loop_stop()
c.disconnect()
