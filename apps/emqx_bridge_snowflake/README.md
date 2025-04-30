# EMQX Snowflake Bridge

This application provides connector and action implementations for the EMQX to integrate with Snowflake as part of the EMQX data integration pipelines.
Users can leverage [EMQX Rule Engine](https://docs.emqx.com/en/enterprise/latest/data-integration/rules.html) to create rules that publish message data to Snowflake.

## Documentation

Refer to [Rules engine](https://docs.emqx.com/en/enterprise/latest/data-integration/rules.html) for the EMQX rules engine introduction.

## Implementation details

At the time of writing, it's not possible to use Snowflake's SDKs which support [Snowpipe Streaming](https://github.com/snowflakedb/snowflake-ingest-java/blob/master/README.md#snowpipe-streaming).  What the SDK does that allows it to have a lower latency seems to involve buffering rows into (Parquet) files before uploading (staging) and ingesting them in bulk using currently undocumented Snowpipe Streaming APIs, which have no equivalent in Snowpipe API.  Here, we instead attempt to use the official, documented ODBC and Snowpipe APIs to provide batch data ingestion.

In Snowflake, files are staged either in internal or external stages (AWS, Azure, GCP).  Internal stages must execute [`PUT`](https://docs.snowflake.com/en/user-guide/data-load-local-file-system-stage) statements using the ODBC API.  In our implementation, we buffer messages/rows using [`emqx_connector_aggregator`](../emqx_connector_aggregator) in container files (currently, only CSV containers are supported), independently on each EMQX node in the cluster, until either a time or a record count threshold is hit.  One or more temporary files might be created locally, depending on the volume of data and configured maximum file limits.  These files are then uploaded via an ODBC connection using a `PUT file://<local path> @<stage>/<action name>` command, via a "delivery", in `emqx_connector_aggregator` terms.  Once all files are staged, we load the data using [Snowpipe's `insertFiles` REST API](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-rest-load).  There's no equivalent ODBC API for this ingestion at this time, except directly specifying `COPY INTO` statements, which require allocating and specifying an [warehouse](https://docs.snowflake.com/en/user-guide/warehouses-overview).  After Snowpipe ingests the data, it'll be available in the table defined in the pipe's DDL `COPY INTO` statement.  Note that there may be multiple deliveries going on concurrently at any given time.

## Contributing

Please see our [contributing.md](../../CONTRIBUTING.md).
