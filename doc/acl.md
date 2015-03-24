
# ACL

## Protocol

Authentication of users and devices

Authorization of access to Server resources

An implementation may restrict access to Server resources based on information provided by the Client such as User Name, Client Identifier, the hostname/IP address of the Client, or the outcome of authentication mechanisms.

Identify a MQTT User: Peername, ClientId, Username


## Access Rule

allow | deny Who subscribe | publish Topic | all

allow {clientid, {regexp, "abcd"}} subscribe "anna"
deny  {clientid, "xxxx"} publish "#"
allow {clientid, "abcd"} publish "#"
allow {peername, "127.0.0.1"} subscribe "$SYS/#"
allow {peername, "127.0.0.1"} subscribe all
allow {clientid, "clientid"} subscribe  "#"
allow {clientid, {regexp, "abcd"}} publish "anna"
allow all subscribe all
deny  all subscribe all
allow all
deny all

