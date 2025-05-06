# Azure Event Hub Data Integration Bridge

This application houses the Azure Event Hub (AEH) Producer data
integration bridge for EMQX Enterprise Edition.  It provides the means
to connect to Azure Event Hub Producer and publish messages to it via
the Kafka protocol.

Currently, our Kafka Producer library (`wolff`) has its own `replayq`
buffering implementation, so this bridge does not require buffer
workers from `emqx_resource`.  It implements the connection management
and interaction without need for a separate connector app, since it's
not used by authentication and authorization applications.

# Documentation links

For more information about Kafka interface for AEH, please see [the
official
docs](https://learn.microsoft.com/en-us/azure/event-hubs/azure-event-hubs-kafka-overview).

# Configurations

Please see [Ingest Data into Azure Event Hub](https://www.emqx.io/docs/en/v5.0/data-integration/data-bridge-azure-event-hub.html) for more detailed info.

# Contributing

Please see our [contributing.md](../../CONTRIBUTING.md).
