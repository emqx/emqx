# EMQX MatrixDB Bridge

[YMatrix](https://www.ymatrix.cn/) is a hyper-converged database product developed by YMatrix based on the PostgreSQL / Greenplum classic open source database. In addition to being able to handle time series scenarios with ease, it also supports classic scenarios such as online transaction processing (OLTP) and online analytical processing (OLAP).

The application is used to connect EMQX and MatrixDB.
User can create a rule and easily ingest IoT data into MatrixDB by leveraging
[EMQX Rules](https://docs.emqx.com/en/enterprise/v5.0/data-integration/rules.html).

<!---

# Documentation

- Refer to [Ingest data into MatrixDB](todo)
  for how to use EMQX dashboard to ingest IoT data into MatrixDB.

- Refer to [EMQX Rules](https://docs.emqx.com/en/enterprise/v5.0/data-integration/rules.html)
  for the EMQX rules engine introduction.

--->

# HTTP APIs

- Several APIs are provided for bridge management, which includes create bridge,
  update bridge, get bridge, stop or restart bridge and list bridges etc.

  Refer to [API Docs - Bridges](https://docs.emqx.com/en/enterprise/v5.0/admin/api-docs.html#tag/Bridges)
  for more detailed information.


# Contributing

Please see our [contributing.md](../../CONTRIBUTING.md).
