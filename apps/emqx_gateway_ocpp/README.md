# emqx-ocpp

OCPP-J 1.6 Gateway for EMQX that implement the Central System for OCPP-J protocol.

## Treat Charge Point as Client of EMQX

In EMQX 4.x, OCPP-J Gateway implement as a protocol Plugin and protocol Module (enterprise only).

All Charge Point connected to OCPP-J Gateway will be treat as a normal Client (like MQTT Client) in EMQX,
you can manage it in Dashboard/HTTP-API/CLI by charge point identity.

The Client Info mapping in OCPP-J Gateway:

- Client ID: presented by charge point identity.
- Username: parsed by the username field for HTTP basic authentication.
- Password: parsed by the password field for HTTP basic authentication.

### Charge Point Authentication

As mentioned in the **ocpp-j-1.6 specification**, Charge Point can use HTTP Basic for
authentication. OCPP-J Gateway extracts the username/password from it and fetches
an approval through EMQX's authentication hooks.

That is, the OCPP-J Gateway uses EMQX's authentication plugin to authorize the Charge Point login.

## Message exchanging among Charge Point, EMQX (Central System) and Third-services

```
                                +----------------+  upstream publish  +---------+
+--------------+   Req/Resp     | OCPP-J Gateway | -----------------> | Third   |
| Charge Point | <------------> | over           |     over Topic     | Service |
+--------------+   over ws/wss  | EMQX           | <----------------- |         |
                                +----------------+  dnstream publish  +---------+
```

Charge Point and OCPP-J Gateway communicate through the specifications defined by OCPP-J.
It is mainly based on Websocket or Websocket TLS.


The OCPP-J Gateway publishes all Charge point messages through EMQX, which are called **Up Stream**.
It consists of two parts:

- Topic: the default topic structure is `ocpp/${clientid}/up/${type}/${action}/${id}`
    * ${clientid}: charge point identity.
    * ${type}: enum with `request`, `response`, `error`
    * ${action}: enum all message type name defined **ocpp 1.6 edtion 2**. i.e: `BootNotification`.
    * ${id}: unique message id parsed by OCPP-J message

- Payload: JSON string defined **ocpp 1.6 edtion 2**. i.e:
    ```json
    {"chargePointVendor":"vendor1","chargePointModel":"model1"}
    ```

The OCPP-J Gateway receives commands from external services by subscribing to EMQX
topics and routing them down to the Charge Point in the format defined by OCPP-J,
which are called **Down Stream**.
It consists of two parts:

- Topic: the default topic structure is `ocpp/${clientid}/dn/${type}/${action}/${id}`
    * The values of these variables are the same as for upstream.
    * To receive such messages, OCPP-J Gateway will add a subscription `ocpp/${clientid}/dn/+/+/+`
      for each Charge point client.

- Payload: JSON string defined **ocpp 1.6 edtion 2**. i.e:
    ```json
    {"currentTime": "2022-06-21T14:20:39+00:00", "interval": 300, "status": "Accepted"}
    ```

### Message Re-transmission

TODO

```
ocpp.awaiting_timeout = 30s

ocpp.retry_interval = 30s
```

### Message Format Checking

TODO
```
#ocpp.message_format_checking = all

#ocpp.json_schema_dir = ${application_priv}/schemas

#ocpp.json_schema_id_prefix = urn:OCPP:1.6:2019:12:
```

## Management and Observability

### Manage Clients

### Observe the messaging state
