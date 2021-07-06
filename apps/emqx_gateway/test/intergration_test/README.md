Integration test for emqx-sn
======

## execute following command
```
make
```

## note

The case4 and case5 are about processing received publish message with qos=-1, which needs the mqtt.sn.enable_qos3 in the emqx_sn.conf to set to on, otherwise these two cases may not get correct return value. And we already have python script to set the mqtt.sn.enable_qos3 to be on automatically, all the user needs to do is to use command "make r" to rebuild before using command "make" to start the test.

