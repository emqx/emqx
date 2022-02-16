
# EMQX Rule Engine

This is the design guide of message routing rule engine for the EMQX Broker.

## Concept

A rule is:

```
when
    Match <conditions> | <predicates>
then
    Select <data> and Take <action>;
```

or:

```
rule "Rule Name"
  when
    rule match
  select
    para1 = val1
    para2 = val2
  then
    action(#{para2 => val1, #para2 => val2})
```

## Architecture

```
          |-----------------|
   P ---->| Message Routing |----> S
          |-----------------|
               |     /|\
              \|/     |
          |-----------------|
          |   Rule Engine   |
          |-----------------|
               |      |
        Backends Services Bridges
```

## Design

```
Event | Message -> Rules -> Actions -> Resources
```

```
   P -> |--------------------|    |---------------------------------------|
        | Messages (Routing) | -> | Rules (Select Data, Match Conditions) |
   S <- |--------------------|    |---------------------------------------|
          |---------|    |-----------|    |-------------------------------|
        ->| Actions | -> | Resources | -> | (Backends, Bridges, WebHooks) |
          |---------|    |-----------|    |-------------------------------|
```



## Rule

A rule consists of a SELECT statement, a topic filter, and a rule action

Rules consist of the following:

- Id
- Name
- Topic
- Description
- Action
- Enabled

The operations on a rule:

- Create
- Enable
- Disable
- Delete



## Action

Actions consist of the following:

- Id
- Name
- For
- App
- Module
- Func
- Args
- Descr

Define a rule action in ADT:

```
action :: Application -> Resource -> Params -> IO ()
```

A rule action:

Module:function(Args)



## Resource

### Resource Name

```
backend:mysql:localhost:port:db
backend:redis:localhost:
webhook:url
bridge:kafka:
bridge:rabbit:localhost
```

### Resource Properties

- Name
- Descr or Description
- Config #{}
- Instances
- State: Running | Stopped

### Resource Management

1. Create Resource
2. List Resources
3. Lookup Resource
4. Delete Resource
5. Test Resource

### Resource State (Lifecircle)

0. Create Resource and Validate a Resource
1. Start/Connect Resource
2. Bind resource name to instance
3. Stop/Disconnect Resource
4. Unbind resource name with instance
5. Is Resource Alive?

### Resource Type

The properties and behaviors of resources is defined by resource types. A resoure type is provided(contributed) by a plugin.

### Resource Type Provider

Provider of resource type is a EMQX Plugin.

### Resource Manager

```
              Supervisor
                   |
                  \|/
Action ----> Proxy(Batch|Write) ----> Connection -----> ExternalResource
  |                                      /|\
  |------------------Fetch----------------|
```



## REST API

Rules API
Actions API
Resources API

## CLI

```
rules list
rules show <RuleId>

rule-actions list
rule-actions show <ActionId>

resources list
resources show <ResourceId>

resource_templates list
resource_templates show <ResourceType>
```

