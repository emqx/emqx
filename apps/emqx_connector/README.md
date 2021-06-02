# emqx_connector

A connector is an object that maintains the data related to external resources.

For example, a mysql connector is an object that maintains all the mysql connection
related parameters (configs) and the TCP connections to the mysql server.

An mysql connector can be used as following:

```
(emqx@127.0.0.1)5> emqx_resource:list_instances_verbose().
[#{config =>
       #{auto_reconnect => true,cacertfile => [],certfile => [],
         database => "mqtt",keyfile => [],password => "public",
         pool_size => 1,
         server => {{127,0,0,1},3306},
         ssl => false,user => "root",verify => false},
   id => <<"mysql-abc">>,mod => emqx_connector_mysql,
   state => #{poolname => 'mysql-abc'},
   status => started}]
(emqx@127.0.0.1)6> emqx_resource:query(<<"mysql-abc">>, {sql, <<"SELECT count(1)">>}).
{ok,[<<"count(1)">>],[[1]]}
```
