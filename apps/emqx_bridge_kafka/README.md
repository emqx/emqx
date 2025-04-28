# Kafka Data Integration Bridge

This application houses the Kafka Producer and Consumer data
integration bridges for EMQX Enterprise Edition.  It provides the
means to connect to Kafka and publish/consume messages to/from it.

Currently, our Kafka Producer library (`wolff`) has its own `replayq`
buffering implementation, so this bridge does not require buffer
workers from `emqx_resource`.  It implements the connection management
and interaction without need for a separate connector app, since it's
not used by authentication and authorization applications.

# Documentation links

For more information about Apache Kafka, please see its [official site](https://kafka.apache.org/).

# Configurations

Please see [Ingest Data into Kafka](https://www.emqx.io/docs/en/v5.0/data-integration/data-bridge-kafka.html) for more detailed info.

# Contributing

Please see our [contributing.md](../../CONTRIBUTING.md).
