# Confluent Data Integration Bridge

This application houses the Confluent Producer data integration bridge for EMQX Enterprise
Edition.  It provides the means to connect to Confluent Producer and publish messages to
it via the Kafka protocol.

Currently, our Kafka Producer library (`wolff`) has its own `replayq` buffering
implementation, so this bridge does not require buffer workers from `emqx_resource`.  It
implements the connection management and interaction without need for a separate connector
app, since it's not used by authentication and authorization applications.

# Documentation links

For more information about Kafka interface for Confluent, please see [the official
docs](https://docs.confluent.io/cloud/current/overview.html).

# Configurations

Please see [Ingest Data into Confluent](https://docs.emqx.com/en/enterprise/v5.3/data-integration/data-bridge-confluent.html) for more detailed info.

# Contributing

Please see our [contributing.md](../../CONTRIBUTING.md).
