# MQTT Protocol Guide

## Server or Broker

A program or device that acts as an intermediary between Clients which publish Application Messages and Clients which have made Subscriptions. 

A Server Accepts Network Connections from Clients.  
Accepts Application Messages published by Clients. 
Processes Subscribe and Unsubscribe requests from Clients.  
Forwards Application Messages that match Client Subscriptions.


Client ----> Broker(Server) ----> Client

Publisher ----> Broker -----> Subscriber

## Subscription and Session

### Subscription

A Subscription comprises a Topic Filter and a maximum QoS. A Subscription is associated with a single Session. A Session can contain more than one Subscription. Each Subscription within a session has a different Topic Filter.

### Session

A stateful interaction between a Client and a Server. Some Sessions last only as long as the Network

Connection, others can span multiple consecutive Network Connections between a Client and a Server.

## Topic Name and Filter

An expression contained in a Subscription, to indicate an interest in one or more topics. A Topic Filter can include wildcard characters.


## Packet Identifier


