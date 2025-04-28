# EMQX RabbitMQ Bridge

[RabbitMQ](https://www.rabbitmq.com/) is a powerful, open-source message broker
that facilitates asynchronous communication between different components of an
application. Built on the Advanced Message Queuing Protocol (AMQP), RabbitMQ
enables the reliable transmission of messages by decoupling the sender and
receiver components. This separation allows for increased scalability,
robustness, and flexibility in application architecture.

RabbitMQ is commonly used for a wide range of purposes, such as distributing
tasks among multiple workers, enabling event-driven architectures, and
implementing publish-subscribe patterns. It is a popular choice for
microservices, distributed systems, and real-time applications, providing an
efficient way to handle varying workloads and ensuring message delivery in
complex environments.

This application is used to connect EMQX and RabbitMQ. User can create a rule
and easily ingest IoT data into RabbitMQ by leveraging
[EMQX Rules](https://docs.emqx.com/en/enterprise/v5.0/data-integration/rules.html).


# Documentation

<!---
- Refer to the [RabbitMQ bridge documentation](https://docs.emqx.com/en/enterprise/v5.0/data-integration/data-bridge-rabbitmq.html)
  for how to use EMQX dashboard to ingest IoT data into RabbitMQ.
--->
- Refer to [EMQX Rules](https://docs.emqx.com/en/enterprise/v5.0/data-integration/rules.html)
  for an introduction to the EMQX rules engine.


# HTTP APIs

- Several APIs are provided for bridge management, which includes create bridge,
  update bridge, get bridge, stop or restart bridge and list bridges etc.

  Refer to [API Docs - Bridges](https://docs.emqx.com/en/enterprise/v5.0/admin/api-docs.html#tag/Bridges) for more detailed information.


# Contributing

Please see our [contributing.md](../../CONTRIBUTING.md).
