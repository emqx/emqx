
# emqx-rule-engine

IoT Rule Engine for EMQX Broker.

## Concept

```
iot rule "Rule Name"
  when
     match TopicFilters and Conditions
  select
    para1 = val1
    para2 = val2
  then
    take action(#{para2 => val1, #para2 => val2})
```

## Architecture

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
        Backends Services Bridges
```

## SQL for Rule query statement

```
select id, time, temperature as t from "topic/a" where t > 50;
```

## License

Copyright (c) 2019-2022 [EMQ Technologies Co., Ltd](https://emqx.io). All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");you may not use this file except in compliance with the License.You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.

