# EMQX NATS Gateway

## Overview

The EMQX NATS Gateway is a protocol gateway that enables EMQX to handle NATS protocol connections. It provides a bridge between NATS clients and EMQX, allowing NATS clients to connect to EMQX and exchange messages using the NATS protocol.

## Features

- Full NATS protocol support
- Message publishing and subscription
- Queue groups support
- Headers support (HPUB/HMSG)
- Connection management
- Error handling
- Protocol frame parsing and serialization

## Protocol Support

The gateway supports the following NATS protocol operations:

### Control Operations
- PING/PONG
- CONNECT
- INFO
- OK
- ERR

### Message Operations
- PUB/HPUB (Publish with/without headers)
- SUB (Subscribe)
- UNSUB (Unsubscribe)
- MSG/HMSG (Message with/without headers)