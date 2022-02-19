# emqx_gateway

EMQX Gateway

## Concept

    EMQX Gateway Management
     - Gateway-Registry (or Gateway Type)
        - *Load
        - *UnLoad
        - *List

     - Gateway
        - *Create
        - *Delete
        - *Update
            - *Stop-And-Start
            - *Hot-Upgrade
        - *Satrt/Enable
        - *Stop/Disable
     - Listener

## ROADMAP

Gateway v0.1: "Basic Functionals"
    - Management support
    - Conn/Frame/Protocol Template
    - Support Stomp/MQTT-SN/CoAP/LwM2M/ExProto

Gateway v0.2: "Integration & Friendly Management"
    - Hooks & Metrics & Statistic
    - HTTP APIs
    - Management in the cluster
    - Integrate with AuthN
    - Integrate with `emqx_config`
    - Improve hocon config
    - Mountpoint & ClientInfo's Metadata
    - The Concept Review

Gateway v0.3: "Fault tolerance and high availability"
    - A common session modoule for message delivery policy
    - The restart mechanism for gateway-instance
    - Consistency of cluster state
    - Configuration hot update

Gateway v1.0: "Best practices for each type of protocol"
    - CoAP
    - Stomp
    - MQTT-SN
    - LwM2M

### Compatible with EMQX

> Why we need to compatible

1. Authentication
2. Hooks/Event system
3. Messages Mode & Rule Engine
4. Cluster registration
5. Metrics & Statistic

> How to do it

>

### User Interface

#### Configurations

```hocon
gateway {

  ## ... some confs for top scope
  ..
  ## End.

  ## Gateway Instances

  lwm2m[.name] {

    ## variable support
    mountpoint: lwm2m/%e/

    lifetime_min: 1s
    lifetime_max: 86400s
    #qmode_time_window: 22
    #auto_observe: off

    #update_msg_publish_condition: contains_object_list

    xml_dir: {{ platform_etc_dir }}/lwm2m_xml

    clientinfo_override: {
        username: ${register.opts.uname}
        password: ${register.opts.passwd}
        clientid: ${epn}
    }

    #authenticator: allow_anonymous
    authenticator: [
      {
        type: auth-http
        method: post
        //?? how to generate clientinfo ??
        params: $client.credential
      }
    ]

    translator: {
      downlink: "dn/#"
      uplink: {
        notify: "up/notify"
        response: "up/resp"
        register: "up/resp"
        update: "up/reps"
      }
    }

    %% ?? listener.$type.name ??
    listener.udp[.name] {
      listen_on: 0.0.0.0:5683
      max_connections: 1024000
      max_conn_rate: 1000
      ## ?? udp keepalive in socket level ???
      #keepalive:
      ## ?? udp proxy-protocol in socket level ???
      #proxy_protocol: on
      #proxy_timeout: 30s
      recbuf: 2KB
      sndbuf: 2KB
      buffer: 2KB
      tune_buffer: off
      #access: allow all
      read_packets: 20
    }

    listener.dtls[.name] {
      listen_on: 0.0.0.0:5684
        ...
    }
  }

  ## The CoAP Gateway
  coap[.name] {

    #enable_stats: on

    authenticator: [
      ...
    ]

    listener.udp[.name] {
      ...
    }

    listener.dtls[.name] {
      ...
    }
}

  ## The Stomp Gateway
  stomp[.name] {

    allow_anonymous: true

    default_user.login: guest
    default_user.passcode: guest

    frame.max_headers: 10
    frame.max_header_length: 1024
    frame.max_body_length: 8192

    listener.tcp[.name] {
        ...
    }

    listener.ssl[.name] {
        ...
    }
  }

  exproto[.name] {

    proto_name: DL-648

    authenticators: [...]

    adapter: {
      type: grpc
      options: {
        listen_on: 9100
      }
    }

    handler: {
      type: grpc
        options: {
          url: <http://127.0.0.1:9001>
        }
    }

    listener.tcp[.name] {
        ...
    }
  }

  ## ============================ Enterpise gateways

  ## The JT/T 808 Gateway
  jtt808[.name] {

    idle_timeout: 30s
    enable_stats: on
    max_packet_size: 8192

    clientinfo_override: {
      clientid: $phone
      username: xxx
      password: xxx
    }

    authenticator: [
      {
        type: auth-http
        method: post
        params: $clientinfo.credential
        }
    ]

    translator: {
      subscribe: [jt808/%c/dn]
      publish: [jt808/%c/up]
    }

    listener.tcp[.name] {
        ...
    }

    listener.ssl[.name] {
        ...
    }
  }

  gbt32960[.name] {

    frame.max_length: 8192
    retx_interval: 8s
    retx_max_times: 3
    message_queue_len: 10

    authenticators: [...]

    translator: {
      ## upstream
      login: gbt32960/${vin}/upstream/vlogin
      logout: gbt32960/${vin}/upstream/vlogout
      informing: gbt32960/${vin}/upstream/info
      reinforming: gbt32960/${vin}/upstream/reinfo
      ## downstream
      downstream: gbt32960/${vin}/dnstream
      response: gbt32960/${vin}/upstream/response
    }

    listener.tcp[.name] {
        ...
    }

    listener.ssl[.name] {
        ...
    }
  }

  privtcp[.name] {

    max_packet_size: 65535
    idle_timeout: 15s

    enable_stats: on

    force_gc_policy: 1000|1MB
    force_shutdown_policy: 8000|800MB

    translator: {
        up_topic: tcp/%c/up
        dn_topic: tcp/%c/dn
    }

    listener.tcp[.name]: {
        ...
    }
  }
}
```

#### CLI

##### Gateway

```bash
## List all started gateway and gateway-instance
emqx_ctl gateway list
emqx_ctl gateway lookup <GatewayId>
emqx_ctl gateway stop   <GatewayId>
emqx_ctl gateway start  <GatewayId>

emqx_ctl gateway-registry re-searching
emqx_ctl gateway-registry list

emqx_ctl gateway-clients list <Type>
emqx_ctl gateway-clients show <Type> <ClientId>
emqx_ctl gateway-clients kick <Type> <ClientId>

## Banned ??
emqx_ctl gateway-banned

## Metrics
emqx_ctl gateway-metrics [<GatewayId>]
```

#### Management by HTTP-API/Dashboard/

#### How to integrate a protocol to your platform

### Develop your protocol gateway

There are 3 way to create your protocol gateway for EMQX 5.0:

1. Use Erlang to create a new emqx plugin to handle all of protocol packets (same as v5.0 before)

2. Based on the emqx-gateway-impl-bhvr and emqx-gateway

3. Use the gRPC Gateway
