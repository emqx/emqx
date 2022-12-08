# emqx_resource

The `emqx_resource` is a behavior that manages configuration specs and runtime states
for resources like mysql or redis backends.

It is intended to be used by the emqx_bridges and all other resources that need CRUD operations
to their configs, and need to initialize the states when creating.

There can be foreign references between resource instances via resource-id.
So they may find each other via this Id.

The main idea of the emqx resource is to put all the `general` code in a common lib, including
the config operations (like config validation, config dump back to files), and the state management.
And we put all the `specific` codes to the callback modules.

See
* `test/emqx_connector_demo.erl` for a minimal `emqx_resource` implementation;
* `test/emqx_resource_SUITE.erl` for examples of `emqx_resource` usage.
