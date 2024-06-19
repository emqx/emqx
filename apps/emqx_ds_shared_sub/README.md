# EMQX Durable Shared Subscriptions

This application makes durable session capable to cooperatively replay messages from a topic.

# General layout and interaction with session

![General layout](docs/images/ds_shared_subs.png)

* The nesting reflects nesting/ownership of entity states.
* The bold arrow represent the [most complex interaction](https://github.com/emqx/eip/blob/main/active/0028-durable-shared-subscriptions.md#shared-subscription-session-handler), between session-side group subscription state machine and the shared subscription leader.

# Contributing

Please see our [contributing.md](../../CONTRIBUTING.md).

# License

EMQ Business Source License 1.1, refer to [LICENSE](BSL.txt).
