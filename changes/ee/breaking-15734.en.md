Improved reliability and throughput of durable sessions.

### Upgrading from 5.x

If durable sessions feature was not previously enabled, the following information can be ignored.

6.0 release changes the internal representation of the durable sessions and messages.
If the cluster was previously running on version 5.x with the feature enabled,
it must be recreated from the clean state.
