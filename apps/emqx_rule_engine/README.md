
# EMQX Rule Engine

The rule engine's goal is to provide a simple and flexible way to transform and
reroute the messages coming to the EMQX broker. For example, one message
containing measurements from multiple sensors of different types can be
transformed into multiple messages.


## Concepts

A rule is quite simple. A rule describes which messages it affects by
specifying a topic filter and a set of conditions that need to be met. If a
message matches the topic filter and all the conditions are met, the rule is
triggered. The rule can then transform the message and route it to a different
topic, or send it to another service (defined by an EMQX bridge). The rule
engine's message data transformation is designed to work well with structured data
such as JSON, avro, and protobuf.


A rule consists of the three parts **MATCH**, **TRANSFORM** and **ACTIONS** that are
described below:

* **MATCH** - The rule's trigger condition. The rule is triggered when a message 
  arrives that matches the topic filter and all the specified conditions are met.
* **TRANSFORM** - The rule's data transformation. The rule can select data from the 
  incoming message and transform it into a new message.
* **ACTIONS** - The rule's action(s). The rule can have one or more actions. The 
  actions are executed when the rule is triggered. The actions can be to route
  the message to a different topic, or send it to another service (defined by
  an EMQX bridge). 



## Architecture

The following diagram shows how the rule engine is integrated with the EMQX
message broker. Incoming messages are checked against the rules, and if a rule
matches, it is triggered with the message as input. The rule can then transform
or split the message and/or route it to a different topic, or send it to another
service (defined by an EMQX bridge).


```
          |-----------------|
 Pub ---->| Message Routing |----> Sub
          |-----------------|
               |     /|\
              \|/     |
          |-----------------|
          |   Rule Engine   |
          |-----------------|
               |      |
           Services Bridges (defined by EMQX bridges)
```

## Domain Specific Language for Rules

The **MATCH** and **TRANSFORM** parts of the rule are specified using a domain
specific language that looks similar to SQL. The following is an example of a
rule engine statement. The `from "topic/a"` part specifies the topic filter
(only messages to the topic `topic/a` will be considered). The `where t > 50`
part specifies the condition that needs to be met for the rule to be triggered.
The `select id, time, temperature as t` part specifies the data transformation
(the selected fields will remain in the transformed message payload). The `as
t` part specifies that the `temperature` field name is changed to `t` in the
output message. The name `t` can also be used in the where part of the rule as
an alias for `t`.


```
select id, time, temperature as t from "topic/a" where t > 50
```

 This just scratches the surface of what is possible with the rule engine. The
 full documentation is available at [EMQX Rule
 Engine](https://www.emqx.io/docs/en/v5.0/data-integration/rules.html). For
 example, there are many built-in functions that can be used in the rule engine
 language to help in doing transformations and matching. One of the [built-in
 functions allows you to run JQ
 queries](https://www.emqx.io/docs/en/v5.0/data-integration/rule-sql-jq.html)
 which allows you to do complex transformations of the message.

