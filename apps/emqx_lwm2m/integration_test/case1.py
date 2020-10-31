


import sys, time
import paho.mqtt.client as mqtt


quit_now = False
DeviceId = "jXtestlwm2m"
test_step = 0

def on_connect(mqttc, userdata, flags, rc):
    global DeviceId
    json = "{\"CmdID\":5,\"Command\":\"Discover\",\"BaseName\":\"/3/0\"}"
    mqttc.publish("lwm2m/"+DeviceId+"/command", json)

def on_message(mqttc, userdata, msg):
    global quit_now, conclusion, test_step
    if msg:
        print(["incoming message topic ", msg.topic, "  payload ", msg.payload])
        
        if msg.topic == 'lwm2m/jXtestlwm2m/response':
            if test_step == 0:
                if msg.payload == '{"CmdID":5,"Command":"Discover","Result":"</3/0>,</3/0/0>,</3/0/1>,</3/0/16>,</3/0/4>"}':
                    json = "{\"CmdID\":6,\"Command\":\"Read\",\"BaseName\":\"/3/0\"}"
                    mqttc.publish("lwm2m/"+DeviceId+"/command", json)
                    test_step = 1
            elif test_step == 1:
                if msg.payload == '{"CmdID":6,"Command":"Read","Result":{"bn":"/3/0","e":[{"n":"0","sv":"Open Mobile Alliance"},{"n":"1","sv":"Lightweight M2M Client"},{"n":"16","sv":"U"}]}}':
                    test_step = 2
                    quit_now = True
    

def on_publish(mqttc, userdata, mid):
    pass



def main():
    global DeviceId, test_step
    timeout = 7
    mqttc = mqtt.Client("test_coap_lwm2m_c02334")
    mqttc.on_message = on_message
    mqttc.on_publish = on_publish
    mqttc.on_connect = on_connect

    mqttc.connect("127.0.0.1", 1883, 120)
    mqttc.subscribe("lwm2m/"+DeviceId+"/response", qos=1)
    mqttc.loop_start()
    while quit_now == False and timeout > 0:
        time.sleep(1)
        timeout = timeout - 1
    mqttc.disconnect()
    mqttc.loop_stop()
    if test_step == 2:
        print("\n\n    CASE1 PASS\n\n")
    else:
        print("\n\n    CASE1 FAIL\n\n")


if __name__ == "__main__":
    main()

    

