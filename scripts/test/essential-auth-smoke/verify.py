#!/usr/bin/env python3
"""Verify built-in-database authentication and acl.conf authorization.

Connects to a running EMQX broker (expected to be configured with a
`built_in_database` authenticator seeded from `bootstrap.csv` and a `file`
authorizer using `acl.conf`) and asserts that:

  * valid credentials connect, wrong password and anonymous are rejected;
  * a subscribe/publish under the user's own subtree is allowed;
  * a subscribe/publish outside it is denied.

Usage: verify.py [host] [port]   (defaults: 127.0.0.1 1883)

Requires the `paho-mqtt` package; works with both 1.x and 2.x.
"""
import sys
import time
import paho.mqtt.client as mqtt

HOST = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 1883
USER, PASSWD = "smoke_user", "smoke_pass"
ALLOWED = f"smoke/{USER}/data"
DENIED = "forbidden/data"

failures = []


def rc_val(rc):
    return getattr(rc, "value", rc)


def check(name, cond):
    print(("PASS" if cond else "FAIL") + f": {name}")
    if not cond:
        failures.append(name)


def new_client(cid):
    try:  # paho-mqtt 2.x
        return mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION1, client_id=cid, protocol=mqtt.MQTTv5
        )
    except AttributeError:  # paho-mqtt 1.x
        return mqtt.Client(client_id=cid, protocol=mqtt.MQTTv5)


def connect(user, pw, cid):
    c = new_client(cid)
    state = {}
    c.on_connect = lambda cl, u, flags, rc, props=None: state.update(rc=rc)
    if user is not None:
        c.username_pw_set(user, pw)
    c.connect(HOST, PORT, 30)
    c.loop_start()
    for _ in range(50):
        if "rc" in state:
            break
        time.sleep(0.1)
    return c, state.get("rc")


def main():
    # 1. valid credentials -> success
    c, rc = connect(USER, PASSWD, "smoke-ok")
    check("valid credentials connect succeeds", rc is not None and rc_val(rc) == 0)
    c.loop_stop()
    c.disconnect()

    # 2. wrong password -> rejected
    c, rc = connect(USER, "wrong", "smoke-badpw")
    check("wrong password rejected", rc is None or rc_val(rc) != 0)
    c.loop_stop()
    c.disconnect()

    # 3. anonymous -> rejected
    c, rc = connect(None, None, "smoke-anon")
    check("anonymous rejected", rc is None or rc_val(rc) != 0)
    c.loop_stop()
    c.disconnect()

    # 4. ACL: subscribe allowed vs denied
    c, rc = connect(USER, PASSWD, "smoke-acl")
    if rc is None or rc_val(rc) != 0:
        check("setup connect for ACL checks", False)
        return
    suback = {}
    c.on_subscribe = lambda cl, u, mid, codes, props=None: suback.__setitem__(mid, codes)

    def sub(topic):
        _, mid = c.subscribe(topic, 1)
        for _ in range(50):
            if mid in suback:
                return rc_val(suback[mid][0])
            time.sleep(0.1)
        return None

    check("subscribe to allowed topic granted", (sub(ALLOWED) or 128) < 128)
    check("subscribe to denied topic refused", (sub(DENIED) or 0) >= 128)

    # 5. ACL: publish allowed delivered, denied dropped
    got = []
    c.on_message = lambda cl, u, msg: got.append(msg.topic)
    c.subscribe(ALLOWED, 1)
    time.sleep(0.3)
    c.publish(ALLOWED, b"hello", 1)
    time.sleep(0.5)
    check("publish to allowed topic delivered", ALLOWED in got)
    c.publish(DENIED, b"nope", 1)
    time.sleep(0.5)
    check("publish to denied topic not delivered", DENIED not in got)
    c.loop_stop()
    c.disconnect()


main()
print(f"\n{'ALL PASSED' if not failures else 'FAILURES: ' + ', '.join(failures)}")
sys.exit(1 if failures else 0)
