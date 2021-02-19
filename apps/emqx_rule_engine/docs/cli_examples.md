#Rule-Engine-CLIs

## Rules

### create

```shell
  $ ./bin/emqx_ctl rules create 'SELECT payload FROM "t/#" username="Steven"' '[{"name":"data_to_webserver", "params": {"$resource": "resource:9093f1cb"}}]' --descr="Msg From Steven to WebServer"

Rule rule:98a75239 created
```

### modify


```shell
  ## update sql, action, description
  $ ./bin/emqx_ctl rules update 'rule:98a75239' \
      -s "select * from \"t/a\" " \
      -a '[{"name":"do_nothing", "fallbacks": []' -g continue \
      -d 'Rule for debug2' \

  ## update sql only
  $ ./bin/emqx_ctl rules update 'rule:98a75239' -s 'SELECT * FROM "t/a"'

  ## disable the rule
  $ ./bin/emqx_ctl rules update 'rule:98a75239' -e false

```

### show

```shell
$ ./bin/emqx_ctl rules show rule:98a75239

rule(id='rule:98a75239', rawsql='SELECT payload FROM "t/#" username="Steven"', actions=[{"name":"data_to_webserver","params":{"$resource":"resource:9093f1cb","url":"http://host-name/chats"}}], enabled='true', description='Msg From Steven to WebServer')
```

### list

```shell
$ ./bin/emqx_ctl rules list

rule(id='rule:98a75239', rawsql='SELECT payload FROM "t/#" username="Steven"', actions=[{"name":"data_to_webserver","params":{"$resource":"resource:9093f1cb","url":"http://host-name/chats"}}], enabled='true', description='Msg From Steven to WebServer')

```

### delete

```shell
$ ./bin/emqx_ctl rules delete 'rule:98a75239'

ok
```

## Actions

### list

```shell
$ ./bin/emqx_ctl rule-actions list

action(name='republish', app='emqx_rule_engine', types=[], params=#{...}, description='Republish a MQTT message to a another topic')
action(name='inspect', app='emqx_rule_engine', types=[], params=#{...}, description='Inspect the details of action params for debug purpose')
action(name='data_to_webserver', app='emqx_web_hook', types=[], params=#{...}, description='Forward Messages to Web Server')
```

### show

```shell
$ ./bin/emqx_ctl rule-actions show 'data_to_webserver'

action(name='data_to_webserver', app='emqx_web_hook', types=['web_hook'], params=#{...}, description='Forward Messages to Web Server')
```

## Resource

### create

```shell
$ ./bin/emqx_ctl resources create 'web_hook' -c '{"url": "http://host-name/chats"}' --descr 'Resource towards http://host-name/chats'

Resource resource:19addfef created
```

### list

```shell
$ ./bin/emqx_ctl resources list

resource(id='resource:19addfef', type='web_hook', config=#{<<"url">> => <<"http://host-name/chats">>}, attrs=undefined, description='Resource towards http://host-name/chats')

```

### list all resources of a type

```shell
$ ./bin/emqx_ctl resources list -t 'web_hook'

resource(id='resource:19addfef', type='web_hook', config=#{<<"url">> => <<"http://host-name/chats">>}, attrs=undefined, description='Resource towards http://host-name/chats')
```

### show

```shell
$ ./bin/emqx_ctl resources show 'resource:19addfef'

resource(id='resource:19addfef', type='web_hook', config=#{<<"url">> => <<"http://host-name/chats">>}, attrs=undefined, description='Resource towards http://host-name/chats')
```

### delete

```shell
$ ./bin/emqx_ctl resources delete 'resource:19addfef'

ok
```

## Resources Types

### list

```shell
$ ./bin/emqx_ctl resource-types list

resource_type(name='built_in', provider='emqx_rule_engine', params=#{...}, on_create={emqx_rule_actions,on_resource_create}, description='The built in resource type for debug purpose')
resource_type(name='web_hook', provider='emqx_web_hook', params=#{...}, on_create={emqx_web_hook_actions,on_resource_create}, description='WebHook Resource')
```

### show

```shell
$ ./bin/emqx_ctl resource-types show built_in

resource_type(name='built_in', provider='emqx_rule_engine', params=#{}, description='The built in resource type for debug purpose')
```

## Rule example using webhook

``` shell
1. Create a webhook resource to URL http://127.0.0.1:9910.
./bin/emqx_ctl resources create 'web_hook' --config '{"url": "http://127.0.0.1:9910", "headers": {"token":"axfw34y235wrq234t4ersgw4t"}, "method": "POST"}'
Resource resource:3128243e created

2. Create a rule using action data_to_webserver, and bind above resource to that action.
./bin/emqx_ctl rules create 'client.connected' 'SELECT clientid as c, username as u.name FROM "#"' '[{"name":"data_to_webserver", "params": {"$resource": "resource:3128243e"}}]' --descr "Forward Connected Events to WebServer"
Rule rule:222b59f7 created
```

Start a simple `Web Server` using `nc`, and then connect to emqx broker using a mqtt client with username = 'Shawn':

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
