# Pulsar Data Integration Bridge

This application houses the Pulsar Producer data integration bridge
for EMQX Enterprise Edition.  It provides the means to connect to
Pulsar and publish messages to it.

Currently, our Pulsar Producer library has its own `replayq` buffering
implementation, so this bridge does not require buffer workers from
`emqx_resource`.  It implements the connection management and
interaction without need for a separate connector app, since it's not
used by authentication and authorization applications.

# Documentation links

For more information on Apache Pulsar, please see its [official
site](https://pulsar.apache.org/).

<!---
# Configurations

Please see [our official
documentation](https://www.emqx.io/docs/en/v5.0/data-integration/data-bridge-pulsar.html)
for more detailed info.
--->

# Contributing

Please see our [contributing.md](../../CONTRIBUTING.md).
