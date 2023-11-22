# emqx_lwm2m

[LwM2M (Lightweight Machine-to-Machine)](https://lwm2m.openmobilealliance.org/)
is a protocol designed for IoT devices and machine-to-machine communication.
It is a lightweight protocol that supports devices with limited processing power and memory.


The **LwM2M Gateway** in EMQX can accept LwM2M clients and translate their events
and messages into MQTT Publish messages.

In the current implementation, it has the following limitations:
- Based UDP/DTLS transport.
- Only supports v1.0.2. The v1.1.x and v1.2.x is not supported yet.
- Not included LwM2M Bootstrap services.

## Quick Start

In EMQX 5.0, LwM2M gateway can be configured and enabled through the Dashboard.

It can also be enabled via the HTTP API, and emqx.conf e.g, In emqx.conf:

```properties
gateway.lwm2m {
  xml_dir = "etc/lwm2m_xml/"
  auto_observe = true
  enable_stats = true
  idle_timeout = "30s"
  lifetime_max = "86400s"
  lifetime_min = "1s"
  mountpoint = "lwm2m/${endpoint_namea}/"
  qmode_time_window = "22s"
  update_msg_publish_condition = "contains_object_list"
  translators {
    command {qos = 0, topic = "dn/#"}
    notify {qos = 0, topic = "up/notify"}
    register {qos = 0, topic = "up/resp"}
    response {qos = 0, topic = "up/resp"}
    update {qos = 0, topic = "up/update"}
  }
  listeners {
    udp {
      default {
        bind = "5783"
        max_conn_rate = 1000
        max_connections = 1024000
      }
    }
  }
}
```

> Note:
> Configuring the gateway via emqx.conf requires changes on a per-node basis,
> but configuring it via Dashboard or the HTTP API will take effect across the cluster.

## Object definations

emqx_lwm2m needs object definitions to parse data from lwm2m devices. Object definitions are declared by organizations in XML format, you could find those XMLs from [LwM2MRegistry](http://www.openmobilealliance.org/wp/OMNA/LwM2M/LwM2MRegistry.html), download and put them into the directory specified by `lwm2m.xml_dir`. If no associated object definition is found, response from device will be discarded and report an error message in log.

More documentations: [LwM2M Gateway](https://www.emqx.io/docs/en/v5.0/gateway/lwm2m.html)
