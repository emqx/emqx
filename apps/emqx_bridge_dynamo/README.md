# EMQX DynamoDB Bridge

[DynamoDB](https://aws.amazon.com/dynamodb/) is a high-performance NoSQL database
service provided by Amazon that's designed for scalability and low-latency access
to structured data.

It's often used in applications that require fast and reliable access to data,
such as mobile, ad tech, and IoT.

The application is used to connect EMQX and DynamoDB.
User can create a rule and easily ingest IoT data into DynamoDB by leveraging
[EMQX Rules](https://docs.emqx.com/en/enterprise/v5.0/data-integration/rules.html).


# Documentation

- Refer to [Ingest data into DynamoDB](https://docs.emqx.com/en/enterprise/v5.0/data-integration/data-bridge-dynamo.html)
  for how to use EMQX dashboard to ingest IoT data into DynamoDB.

- Refer to [Rules engine](https://docs.emqx.com/en/enterprise/v5.0/data-integration/rules.html)
  for the EMQX rules engine introduction.


# HTTP APIs

- Several APIs are provided for bridge management, which includes create bridge,
  update bridge, get bridge, stop or restart bridge and list bridges etc.

  Refer to [API Docs - Bridges](https://docs.emqx.com/en/enterprise/v5.0/admin/api-docs.html#tag/Bridges)
  for more detailed information.


# Contributing

Please see our [contributing.md](../../CONTRIBUTING.md).
