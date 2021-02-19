#Rule-Engine-APIs

## ENVs

APPSECRET="88ebdd6569afc:Mjg3MzUyNTI2Mjk2NTcyOTEwMDEwMDMzMTE2NTM1MTkzNjA"

## Rules

### test sql

$ curl -v --basic -u $APPSECRET -k 'http://localhost:8081/api/v4/rules?test' -d \
'{"rawsql":"select * from \"message.publish\" where topic=\"t/a\"","ctx":{}}'



### create

```shell
$ curl -v --basic -u $APPSECRET -k 'http://localhost:8081/api/v4/rules' -d \
'{"rawsql":"select * from \"t/a\"","actions":[{"name":"inspect","params":{"a":1}}],"description":"test-rule"}'

{"code":0,"data":{"actions":[{"name":"inspect","params":{"a":1}}],"description":"test-rule","enabled":true,"id":"rule:bc987915","rawsql":"select * from \"t/a\""}}

## with a resource id in the action args
$ curl -v --basic -u $APPSECRET -k 'http://localhost:8081/api/v4/rules' -d \
'{"rawsql":"select * from \"t/a\"","actions":[{"name":"inspect","params":{"$resource":"resource:3a7b44a1"}}],"description":"test-rule"}'

{"code":0,"data":{"actions":[{"name":"inspect","params":{"$resource":"resource:3a7b44a1","a":1}}],"description":"test-rule","enabled":true,"id":"rule:6fce0ca9","rawsql":"select * from \"t/a\""}}
```

### modify

```shell
## modify all of the params
$ curl -XPUT -v --basic -u $APPSECRET -k 'http://localhost:8081/api/v4/rules/rule:bc987915' -d \
'{"rawsql":"select * from \"t/a\"","actions":[{"name":"inspect","params":{"a":1}}],"description":"test-rule"}'

## modify some of the params: disable it
$ curl -XPUT -v --basic -u $APPSECRET -k 'http://localhost:8081/api/v4/rules/rule:bc987915' -d \
'{"enabled": false}'

## modify some of the params: add fallback actions
$ curl -XPUT -v --basic -u $APPSECRET -k 'http://localhost:8081/api/v4/rules/rule:bc987915' -d \
'{"actions":[{"name":"inspect","params":{"a":1}, "fallbacks": [{"name":"donothing"}]}]}'
```

### show

```shell
$ curl -v --basic -u $APPSECRET -k 'http://localhost:8081/api/v4/rules/rule:bc987915'

{"code":0,"data":{"actions":[{"name":"inspect","params":{"a":1}}],"description":"test-rule","enabled":true,"id":"rule:bc987915","rawsql":"select * from \"t/a\""}}
```

### list

```shell
$ curl -v --basic -u $APPSECRET -k http://localhost:8081/api/v4/rules

{"code":0,"data":[{"actions":[{"name":"inspect","params":{"a":1}}],"description":"test-rule","enabled":true,"id":"rule:bc987915","rawsql":"select * from \"t/a\""},{"actions":[{"name":"inspect","params":{"$resource":"resource:3a7b44a1","a":1}}],"description":"test-rule","enabled":true,"id":"rule:6fce0ca9","rawsql":"select * from \"t/a\""}]}
```

### delete

```shell
$ curl -XDELETE -v --basic -u $APPSECRET -k 'http://localhost:8081/api/v4/rules/rule:bc987915'

{"code":0}
```

## Actions

### list

```shell
$ curl -v --basic -u $APPSECRET -k http://localhost:8081/api/v4/actions

{"code":0,"data":[{"app":"emqx_rule_engine","description":"Republish a MQTT message to a another topic","name":"republish","params":{...},"types":[]},{"app":"emqx_rule_engine","description":"Inspect the details of action params for debug purpose","name":"inspect","params":{},"types":[]},{"app":"emqx_web_hook","description":"Forward Messages to Web Server","name":"data_to_webserver","params":{...},"types":["web_hook"]}]}
```

### show

```shell
$ curl -v --basic -u $APPSECRET -k 'http://localhost:8081/api/v4/actions/inspect'

{"code":0,"data":{"app":"emqx_rule_engine","description":"Debug Action","name":"inspect","params":{"$resource":"built_in"}}}
```

## Resource Types

### list

```shell
$ curl -v --basic -u $APPSECRET -k 'http://localhost:8081/api/v4/resource_types'

{"code":0,"data":[{"description":"Debug resource type","name":"built_in","params":{},"provider":"emqx_rule_engine"}]}
```

### list all resources of a type

```shell
$ curl -v --basic -u $APPSECRET -k 'http://localhost:8081/api/v4/resource_types/built_in/resources'

{"code":0,"data":[{"attrs":"undefined","config":{"a":1},"description":"test-rule","id":"resource:71df3086","type":"built_in"}]}
```

### show

```shell
$ curl -v --basic -u $APPSECRET -k 'http://localhost:8081/api/v4/resource_types/built_in'

{"code":0,"data":{"description":"Debug resource type","name":"built_in","params":{},"provider":"emqx_rule_engine"}}
```



## Resources

### create

```shell
$ curl -v --basic -u $APPSECRET -k 'http://localhost:8081/api/v4/resources' -d \
'{"type": "built_in", "config": {"a":1}, "description": "test-resource"}'

{"code":0,"data":{"attrs":"undefined","config":{"a":1},"description":"test-resource","id":"resource:71df3086","type":"built_in"}}
```

### start

```shell
$ curl -XPOST -v --basic -u $APPSECRET -k 'http://localhost:8081/api/v4/resources/resource:71df3086'

{"code":0}
```

### list

```shell
$ curl -v --basic -u $APPSECRET -k 'http://localhost:8081/api/v4/resources'

{"code":0,"data":[{"attrs":"undefined","config":{"a":1},"description":"test-resource","id":"resource:71df3086","type":"built_in"}]}
```

### show

```shell
$ curl -v --basic -u $APPSECRET -k 'http://localhost:8081/api/v4/resources/resource:71df3086'

{"code":0,"data":{"attrs":"undefined","config":{"a":1},"description":"test-resource","id":"resource:71df3086","type":"built_in"}}
```

### get resource status

```shell
curl -v --basic -u $APPSECRET -k 'http://localhost:8081/api/v4/resource_status/resource:71df3086'

{"code":0,"data":{"is_alive":true}}
```

### delete

```shell
$ curl -XDELETE -v --basic -u $APPSECRET -k 'http://localhost:8081/api/v4/resources/resource:71df3086'

{"code":0}
```

## Rule example using webhook

``` shell

$ curl -v --basic -u $APPSECRET -k 'http://localhost:8081/api/v4/resources' -d \
'{"type": "web_hook", "config": {"url": "http://127.0.0.1:9910", "headers": {"token":"axfw34y235wrq234t4ersgw4t"}, "method": "POST"}, "description": "web hook resource-1"}'

{"code":0,"data":{"attrs":"undefined","config":{"headers":{"token":"axfw34y235wrq234t4ersgw4t"},"method":"POST","url":"http://127.0.0.1:9910"},"description":"web hook resource-1","id":"resource:8531a11f","type":"web_hook"}}

curl -v --basic -u $APPSECRET -k 'http://localhost:8081/api/v4/rules' -d \
'{"rawsql":"SELECT clientid as c, username as u.name FROM \"#\"","actions":[{"name":"data_to_webserver","params":{"$resource": "resource:8531a11f"}}],"description":"Forward connected events to webhook"}'

{"code":0,"data":{"actions":[{"name":"data_to_webserver","params":{"$resource":"resource:8531a11f","headers":{"token":"axfw34y235wrq234t4ersgw4t"},"method":"POST","url":"http://127.0.0.1:9910"}}],"description":"Forward connected events to webhook","enabled":true,"id":"rule:4fe05936","rawsql":"select * from \"#\""}}
```

Start a `web server` using `nc`, and then connect to emqx broker using a mqtt client with username = 'Shawn':

```shell
$ echo -e "HTTP/1.1 200 OK\n\n $(date)" | nc -l 127.0.0.1 9910

POST / HTTP/1.1
content-type: application/json
content-length: 48
te:
host: 127.0.0.1:9910
connection: keep-alive
token: axfw34y235wrq234t4ersgw4t

{"c":"clientId-bP70ymeIyo","u":{"name":"Shawn"}
```
