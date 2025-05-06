# EMQX OpenTSDB Bridge

[OpenTSDB](http://opentsdb.net) is a distributed, scalable Time Series Database (TSDB) written on top of HBase.

OpenTSDB was written to address a common need: store, index and serve metrics collected from computer systems (network gear, operating systems, applications) at a large scale, and make this data easily accessible and graphable.

OpenTSDB allows you to collect thousands of metrics from tens of thousands of hosts and applications, at a high rate (every few seconds).

OpenTSDB will never delete or downsample data and can easily store hundreds of billions of data points.

The application is used to connect EMQX and OpenTSDB. User can create a rule and easily ingest IoT data into OpenTSDB by leveraging the
[EMQX Rules](https://docs.emqx.com/en/enterprise/v5.0/data-integration/rules.html).


# Documentation

- Refer to [EMQX Rules](https://docs.emqx.com/en/enterprise/v5.0/data-integration/rules.html)
  for the EMQX rules engine introduction.


# HTTP APIs

- Several APIs are provided for bridge management, which includes create bridge,
  update bridge, get bridge, stop or restart bridge and list bridges etc.

  Refer to [API Docs - Bridges](https://docs.emqx.com/en/enterprise/v5.0/admin/api-docs.html#tag/Bridges) for more detailed information.


# Contributing

Please see our [contributing.md](../../CONTRIBUTING.md).
