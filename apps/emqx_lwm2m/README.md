
# LwM2M Gateway for the EMQX Broker.

[The LwM2M Specifications](http://www.openmobilealliance.org/release/LightweightM2M) is a Lightweight Machine to Machine protocol.

With `emqx_lwm2m`, user is able to send LwM2M commands(READ/WRITE/EXECUTE/...) and get LwM2M response in MQTT way. `emqx_lwm2m` transforms data between MQTT and LwM2M protocol.

emqx_lwm2m needs object definitions to parse data from lwm2m devices. Object definitions are declared by organizations in XML format, you could find those XMLs from [LwM2MRegistry](http://www.openmobilealliance.org/wp/OMNA/LwM2M/LwM2MRegistry.html), download and put them into the directory specified by `lwm2m.xml_dir`. If no associated object definition is found, response from device will be discarded and report an error message in log.

## Load emqx_lwm2m

```
./bin/emqx_ctl plugins load emqx_lwm2m
```

## Test emqx-lwm2m using *wakaama*

[wakaama](https://github.com/eclipse/wakaama) is an easy-to-use lwm2m client command line tool.

Start *lwm2mclient* using an endpoint name `ep1`:
```
./lwm2mclient -n ep1 -h 127.0.0.1 -p 5683 -4
```

To send an LwM2M DISCOVER command to *lwm2mclient*, publish an MQTT message to topic `lwm2m/<epn>/dn` (where `<epn>` is the endpoint name of the client), with following payload:

```json
{
    "reqID": "2",
    "msgType": "discover",
    "data": {
        "path": "/3/0"
    }
}
```

The MQTT message will be translated to an LwM2M DISCOVER command and sent to the *lwm2mclient*. Then the response of *lwm2mclient* will be in turn translated to an MQTT message, with topic `lwm2m/<epn>/up/resp`, with following payload:

```json
{
    "reqID": "2",
    "msgType": "discover",
    "data": {
        "code":"2.05",
        "codeMsg": "content",
        "content": [
            "</3/0>;dim=8",
            "</3/0/0>",
            "</3/0/1>",
            "</3/0/4>",
            "</3/0/16>"
        ]
    }
}
```

## LwM2M <--> MQTT Mapping

### Register/Update (LwM2M Client Registration Interface)

- **LwM2M Register and Update message will be converted to following MQTT message:**

  - **Method:** PUBLISH
  - **Topic:** `lwm2m/{?EndpointName}/up/resp` (configurable)
  - **Payload**:
    - MsgType **register** and **update**:
      ```json
      {
          "msgType": {?MsgType},
          "data": {
              "ep": {?EndpointName},
              "lt": {?LifeTime},
              "sms": {?MSISDN},
              "lwm2m": {?Lwm2mVersion},
              "b": {?Binding},
              "alternatePath": {?AlternatePath},
              "objectList": {?ObjectList}
          }
      }
      ```
      - {?EndpointName}: String, the endpoint name of the LwM2M client
      - {?MsgType}: String, could be:
        - "register": LwM2M Register
        - "update": LwM2M Update
      - "data" contains the query options and the object-list of the register message
      - The *update* message is only published if the object-list changed.

### Downlink Command and Uplink Response (LwM2M Device Management & Service Enablement Interface)

- **To send a downlink command to device, publish following MQTT message:**
  - **Method:** PUBLISH
  - **Topic:** `lwm2m/{?EndpointName}/dn`
  - **Request Payload**:
    ```json
    {
        "reqID": {?ReqID},
        "msgType": {?MsgType},
        "data": {?Data}
    }
    ```
    - {?ReqID}: Integer, request-id, used for matching the response to the request
    - {?MsgType}: String, can be one of the following:
      - "read": LwM2M Read
      - "discover": LwM2M Discover
      - "write": LwM2M Write
      - "write-attr": LwM2M Write Attributes
      - "execute": LwM2M Execute
      - "create": LwM2M Create
      - "delete": LwM2M Delete
    - {?Data}: Json Object, its value depends on the {?MsgType}:
      - **If {?MsgType} = "read" or "discover"**:
        ```json
        {
            "path": {?ResourcePath}
        }
        ```
        - {?ResourcePath}: String, LwM2M full resource path. e.g. "3/0", "/3/0/0", "/3/0/6/0"
      - **If {?MsgType} = "write" (single write)**:
        ```json
        {
            "path": {?ResourcePath},
            "type": {?ValueType},
            "value": {?Value}
        }
        ```
        - {?ValueType}: String, can be: "Time", "String", "Integer", "Float", "Boolean", "Opaque", "Objlnk"
        - {?Value}: Value of the resource, depends on "type".
      - **If {?MsgType} = "write" (batch write)**:
        ```json
        {
            "basePath": {?BasePath},
            "content": [
                {
                    "path": {?ResourcePath},
                    "type": {?ValueType},
                    "value": {?Value}
                }
            ]
        }
        ```
        - The full path is concatenation of "basePath" and "path".
      - **If {?MsgType} = "write-attr"**:
        ```json
        {
            "path": {?ResourcePath},
            "pmin": {?PeriodMin},
            "pmax": {?PeriodMax},
            "gt": {?GreaterThan},
            "lt": {?LessThan},
            "st": {?Step}
        }
        ```
        - {?PeriodMin}: Number, LwM2M Notification Class Attribute - Minimum Period.
        - {?PeriodMax}: Number, LwM2M Notification Class Attribute - Maximum Period.
        - {?GreaterThan}: Number, LwM2M Notification Class Attribute - Greater Than.
        - {?LessThan}: Number, LwM2M Notification Class Attribute - Less Than.
        - {?Step}: Number, LwM2M Notification Class Attribute - Step.

      - **If {?MsgType} = "execute"**:
        ```json
        {
            "path": {?ResourcePath},
            "args": {?Arguments}
        }
        ```
        - {?Arguments}: String, LwM2M Execute Arguments.

      - **If {?MsgType} = "create"**:
        ```json
        {
            "basePath": "/{?ObjectID}",
            "content": [
                {
                    "path": {?ResourcePath},
                    "type": {?ValueType},
                    "value": {?Value}
                }
            ]
        }
        ```
        - {?ObjectID}: Integer, LwM2M Object ID

      - **If {?MsgType} = "delete"**:
        ```json
        {
            "path": "{?ObjectID}/{?ObjectInstanceID}"
        }
        ```
        - {?ObjectInstanceID}: Integer, LwM2M Object Instance ID

- **The response of LwM2M will be converted to following MQTT message:**
  - **Method:** PUBLISH
  - **Topic:** `"lwm2m/{?EndpointName}/up/resp"`
  - **Response Payload:**

  ```json
  {
      "reqID": {?ReqID},
      "imei": {?IMEI},
      "imsi": {?IMSI},
      "msgType": {?MsgType},
      "data": {?Data}
  }
  ```

  - {?MsgType}: String, can be:
    - "read": LwM2M Read
    - "discover": LwM2M Discover
    - "write": LwM2M Write
    - "write-attr": LwM2M Write Attributes
    - "execute": LwM2M Execute
    - "create": LwM2M Create
    - "delete": LwM2M Delete
    - **"ack"**: [CoAP Empty ACK](https://tools.ietf.org/html/rfc7252#section-5.2.2)
  - {?Data}: Json Object, its value depends on {?MsgType}:
    - **If {?MsgType} = "write", "write-attr", "execute", "create", "delete", or "read"(when response without content)**:
      ```json
      {
            "code": {?StatusCode},
            "codeMsg": {?CodeMsg},
            "reqPath": {?RequestPath}
      }
      ```
      - {?StatusCode}: String, LwM2M status code, e.g. "2.01", "4.00", etc.
      - {?CodeMsg}: String, LwM2M response message, e.g. "content", "bad_request"
      - {?RequestPath}: String, the requested "path" or "basePath"

    - **If {?MsgType} = "discover"**:
      ```json
      {
          "code": {?StatusCode},
          "codeMsg": {?CodeMsg},
          "reqPath": {?RequestPath},
          "content": [
              {?Link},
              ...
          ]
      }
      ```
      - {?Link}: String(LwM2M link format) e.g. `"</3>"`, `"<3/0/1>;dim=8"`

    - **If {?MsgType} = "read"(when response with content)**:
      ```json
      {
          "code": {?StatusCode},
          "codeMsg": {?CodeMsg},
          "content": {?Content}
      }
      ```
      - {?Content}
        ```json
        [
            {
                "path": {?ResourcePath},
                "value": {?Value}
            }
        ]
        ```

    - **If {?MsgType} = "ack", "data" does not exists**

### Observe (Information Reporting Interface - Observe/Cancel-Observe)

- **To observe/cancel-observe LwM2M client, send following MQTT PUBLISH:**
  - **Method:** PUBLISH
  - **Topic:** `lwm2m/{?EndpointName}/dn`
  - **Request Payload**:
    ```json
    {
        "reqID": {?ReqID},
        "msgType": {?MsgType},
        "data": {
            "path": {?ResourcePath}
        }
    }
    ```
    - {?ResourcePath}: String, the LwM2M resource to be observed/cancel-observed.
    - {?MsgType}: String, can be:
      - "observe": LwM2M Observe
      - "cancel-observe": LwM2M Cancel Observe
    - {?ReqID}: Integer, request-id, is the {?ReqID} in the request

- **Responses will be converted to following MQTT message:**
  - **Method:** PUBLISH
  - **Topic:** `lwm2m/{?EndpointName}/up/resp`
  - **Response Payload**:
    ```json
    {
        "reqID": {?ReqID},
        "msgType": {?MsgType},
        "data": {
            "code": {?StatusCode},
            "codeMsg": {?CodeMsg},
            "reqPath": {?RequestPath},
            "content": [
                {
                    "path": {?ResourcePath},
                    "value": {?Value}
                }
            ]
        }
    }
    ```
    - {?MsgType}: String, can be:
      - "observe": LwM2M Observe
      - "cancel-observe": LwM2M Cancel Observe
      - **"ack"**: [CoAP Empty ACK](https://tools.ietf.org/html/rfc7252#section-5.2.2)

### Notification (Information Reporting Interface - Notify)

- **The notifications from LwM2M clients will be converted to MQTT PUBLISH:**
  - **Method:** PUBLISH
  - **Topic:** `lwm2m/{?EndpiontName}/up/notify`
  - **Notification Payload**:
    ```json
    {
        "reqID": {?ReqID},
        "msgType": {?MsgType},
        "seqNum": {?ObserveSeqNum},
        "data": {
            "code": {?StatusCode},
            "codeMsg": {?CodeMsg},
            "reqPath": {?RequestPath},
            "content": [
                {
                    "path": {?ResourcePath},
                    "value": {?Value}
                }
            ]
        }
    }
    ```
    - {?MsgType}: String, must be "notify"
    - {?ObserveSeqNum}: Number, value of "Observe" option in CoAP message
    - "content": same to the "content" field contains in the response of "read" command

## Feature limitations

- emqx_lwm2m implements LwM2M gateway to EMQX, not a full-featured and independent LwM2M server.
- emqx_lwm2m does not include LwM2M bootstrap server.
- emqx_lwm2m supports UDP binding, no SMS binding yet.
- Firmware object is not fully supported now since mqtt to coap block-wise transfer is not available.
- Object Versioning is not supported now.

## DTLS

emqx-lwm2m support DTLS to secure UDP data.

Please config lwm2m.certfile and lwm2m.keyfile in emqx_lwm2m.conf. If certfile or keyfile are invalid, DTLS will be turned off and you could read a error message in the log.

## License

Apache License Version 2.0

## Author

EMQX-Men Team.
