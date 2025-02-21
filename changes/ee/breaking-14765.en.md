Added extra validation for using Named Instances in SQL Server Connector.  Previously, we could not infer when the user furnished an explicit port for SQL Server, and always added the default port if not explicitly defined.

For Named Instances, we need to explicitly define a port to connect to when connecting with the ODBC driver. And the driver happily connects to whatever instance is running on that port, completely ignoring the given Instance Name, if any.

Now, we impose that the port is to be explicitly defined when an instance name is given, and we also attempt to infer differences between desired and connected instance names during health checks.
