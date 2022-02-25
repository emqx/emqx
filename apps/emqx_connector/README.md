# emqx_connector

This application is a collection of `connectors`.

A `connector` is a callback module of `emqx_resource` that maintains the data related to
external resources. Put all resource related callback modules in a single application is good as
we can put some util functions/modules here for reusing purpose.

For example, a MySQL connector is an emqx resource that maintains all the MySQL connection
related parameters (configs) and the TCP connections to the MySQL server.

An MySQL connector can be used as following:

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
   status => connected}]
(emqx@127.0.0.1)6> emqx_resource:query(<<"mysql-abc">>, {sql, <<"SELECT count(1)">>}).
{ok,[<<"count(1)">>],[[1]]}
```
