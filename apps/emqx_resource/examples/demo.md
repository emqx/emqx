---
theme: gaia
color: #000
colorSecondary: #333
backgroundColor: #fff
backgroundImage: url('https://marp.app/assets/hero-background.jpg')
paginate: true
marp: true
---

<!-- _class: lead -->

# EMQX Resource

---

## What is it for

The [emqx_resource](https://github.com/terry-xiaoyu/emqx_resource) for managing configurations and runtime states for dashboard components .

![bg right](https://docs.emqx.cn/assets/img/rule_action_1@2x.73766093.png)

---

<!-- _class: lead -->

# The Demo

The little log tracer

---

- The hocon schema file (log_tracer_schema.erl):

https://github.com/terry-xiaoyu/emqx_resource/blob/main/examples/log_tracer_schema.erl

- The callback file (log_tracer.erl):

https://github.com/terry-xiaoyu/emqx_resource/blob/main/examples/log_tracer.erl

---

Start the demo log tracer

```
./demo.sh
```

Load instance from config files (auto loaded)

```
## This will load all of the "*.conf" file under that directory:

emqx_resource:load_instances("./_build/default/lib/emqx_resource/examples").
```

The config file is validated against the schema (`*_schema.erl`) before loaded.

---

# List Types and Instances

- To list all the available resource types:

```
emqx_resource:list_types().
emqx_resource:list_instances().
```

- And there's `*_verbose` versions for these `list_*` APIs:

```
emqx_resource:list_types_verbose().
emqx_resource:list_instances_verbose().
```

---
# Instance management

- To get a resource types and instances:

```
emqx_resource:get_type(log_tracer).
emqx_resource:get_instance("log_tracer_clientid_shawn").
```

- To create a resource instances:

```
emqx_resource:create("log_tracer2", log_tracer,
#{bulk => <<"1KB">>,cache_log_dir => <<"/tmp">>,
  cache_logs_in => <<"memory">>,chars_limit => 1024,
  condition => #{<<"app">> => <<"emqx">>},
  enable_cache => true,level => debug}).
```

---

- To update a resource:

```
emqx_resource:update("log_tracer2", log_tracer, #{bulk => <<"100KB">>}, []).
```

- To delete a resource:

```
emqx_resource:remove("log_tracer2").
```

---

<!-- _class: lead -->

# HTTP APIs Demo

---

# Get a log tracer

To list current log tracers:

```
curl -s -XGET 'http://localhost:9900/log_tracer' | jq .
```

---

## Update or Create

To update an existing log tracer or create a new one:

```
INST='{
  "resource_type": "log_tracer",
  "config": {
    "condition": {
      "app": "emqx"
    },
    "level": "debug",
    "cache_log_dir": "/tmp",
    "bulk": "10KB",
    "chars_limit": 1024
  }
}'
curl -sv -XPUT 'http://localhost:9900/log_tracer/log_tracer2' -d $INST | jq .
```
